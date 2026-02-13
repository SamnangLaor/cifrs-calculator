from app.IFRS.Impairment.Calculation.models 	import *
from app.IFRS.Impairment.Group.models 			import MKT_IMP_GROUP
from sqlalchemy.sql     	 					import func
from sqlalchemy 								import case
from collections 								import defaultdict
import numpy 										as np
from datetime 									import date
from app.LoanContract.models 					import MKT_LOAN_CONTRACT
from copy 										import deepcopy
from app.IFRS.tools.mktsetting 					import getIfrsSetting, getAuditrial
import app.tools.mktaccounting 						as mktaccounting
import app.tools.mktsetting 						as mktsetting
from app 										import db
import random
import app.tools.mktpdcollection 						as mktpdcollection
# from app.IFRS.tools.mktimpdatacollection 		import getAuditrial


def getAgingSetting():
	PARPeriod = [str(Obj).split("*")[0] for Obj in str(getIfrsSetting().PARPeriod).splitlines()]
	return PARPeriod


def getPDByNumDay(NumDayDue):
 	"""
		This function will return:
		 	PD1 : Period 1
		 	PD2 : Period 2 .....
 	"""

  PARPeriod = getAgingSetting()

  if NumDayDue == 0 : return "CurrentPD"

  for Index, Value in enumerate(PARPeriod) :
    Value = Value.replace("<=", "1>").replace(">=", "")
		Aging = Value.split(">")

		if len(Aging) == 2 :
			if int(NumDayDue) in range(int(Aging[0]), int(Aging[1])+1) :
				return "PD%s"%str(Index+1)

		elif len(Aging) == 1 :
			if int(NumDayDue) >= int(Aging[0]) :
				return "PD%s"%str(Index+1)


def getAgingByNumDay(NumDayDue):
	"""
		This function will return:
		 	Aging1DueAmt 	: Aging1 Due Amount
		 	Aging2DueAmt 	: Aging2 Due Amount .....
 	"""

	PARPeriod = getAgingSetting()

	if NumDayDue == 0 : return "CurrentDueAmt"

	for Index, Value in enumerate(PARPeriod) :
		Value = Value.replace("<=", "1>").replace(">=", "")
		Aging = Value.split(">")

		if len(Aging) == 2 :
			if int(NumDayDue) in range(int(Aging[0]), int(Aging[1])+1) :
				return "Aging%sDueAmt"%str(Index+1)

		elif len(Aging) == 1 :
			if int(NumDayDue) >= int(Aging[0]) :
				return "Aging%sDueAmt"%str(Index+1)


class ImpairmentCalculator(object):
	"""
		LoanPortfolio data sample :
		Column1: Month End Date
		Column2: Impairment Group
		Column3: Outstanding Amount
		----------------------------------------------------------------------------
		[
			('2019-01-31', '2', Decimal('1512379.700000000'), 'USD'),
			('2019-01-31', '1', Decimal('3149976.300000000'), 'KHR'),
			('2019-01-31', '3', Decimal('3677780.200000000'), 'USD'),
			...
		]

		PARAging data sample :
		Column1: Impairment Group ID
		Column2: Mont End Date
		Column3: PD Amount of 1-30 days due
		Column4: PD Amount of 31-60 days due
		Column5: PD Amount of 61-90 days due
		Column6: PD Amount of more than 90 days due
		----------------------------------------------------------------------------
		[
			('1', '2016-01-31', Decimal('108959.000000000'), Decimal('30026.000000000'), Decimal('13976.000000000'), Decimal('36281.000000000'), 'USD'),
			('2', '2016-01-31', Decimal('6333.000000000'), Decimal('0E-9'), Decimal('0E-9'), Decimal('1641.000000000'), Decimal('0E-9'), Decimal('0E-9'), 'USD'),
			('3', '2016-01-31', Decimal('207965.000000000'), Decimal('3889.000000000'), Decimal('0E-9'), Decimal('2241.000000000'), 'USD'),
			('2', '2016-02-29', Decimal('46963.000000000'), Decimal('3070.000000000'), Decimal('0E-9'), Decimal('0E-9'), 'USD'),
			('1', '2016-02-29', Decimal('125007.000000000'), Decimal('39768.000000000'), Decimal('18242.000000000'), Decimal('32449.000000000'), 'USD'),
			('3', '2016-02-29', Decimal('110911.000000000'), Decimal('4711.000000000'), Decimal('0E-9'), Decimal('2241.000000000'), 'USD'),
			...
		]

		PARCollection data sample :
		Column1: Loan ID
		Column2: Impairment Group ID
		Column3: Month End Date
		Column4: Date of Default
		Colunn5: Collected Amount
		Column6: EIR rate in percentage
		----------------------------------------------------------------------------
		[
			('AAAA-002', '3', '2019-02-28', '2019-01-31', Decimal('74.000000000'), '18.00', 'USD'),
			('AAAA-005', '2', '2019-02-28', '2019-01-31', Decimal('25.000000000'), '34.00', 'USD'),
			('AAAA-002', '3', '2019-03-31', '2019-01-31', Decimal('77.000000000'), '18.00', 'USD'),
			('AAAA-003', '1', '2019-03-31', '2019-01-31', Decimal('15.000000000'), '18.00', 'USD'),
			('AAAA-006', '1', '2019-03-31', '2019-02-28', Decimal('55.000000000'), '22.00', 'USD'),

			...
		]

	"""
	_ReportingRate = mktaccounting.getReportingRateObj()
	_BaseCurrency = mktsetting.getAccSetting().BaseCurrency

	def __init__(self, LoanPortfolio=[], PARAging=[], PARCollection=[], RiskPortfolio=[]):
		super(ImpairmentCalculator, self).__init__()
		self.LoanPortfolio 	= LoanPortfolio
		self.PARAging 		= PARAging
		self.PARCollection 	= PARCollection
		self.RiskPortfolio 	= RiskPortfolio


	def getTLPByGroup(self):
		"""
			TLP = Total Loan Protfolio
			Data to be return sample:
			------------------------------------------------------------------------
			{'1': [
					{'MonthEndDate': '2019-01-31', 'OutstandingAmount': 3149976.3, 'Currency':'USD', Branch:'HOT'},
					{'MonthEndDate': '2019-02-28', 'OutstandingAmount': 3121862.4, 'Currency':'USD', Branch:'HOT'},
					{'MonthEndDate': '2019-03-31', 'OutstandingAmount': 3172227.2, 'Currency':'USD', Branch:'HOT'},
					...
					],
			 '2': [
			 		{'MonthEndDate': '2019-01-31', 'OutstandingAmount': 1512379.7, 'Currency':'USD', Branch:'HOT'},
			 		{'MonthEndDate': '2019-02-28', 'OutstandingAmount': 1460805.3, 'Currency':'USD', Branch:'HOT'},
			 		{'MonthEndDate': '2019-03-31', 'OutstandingAmount': 1463068.3, 'Currency':'USD', Branch:'HOT'},
			 		...],
			 ...
			}

			'1' and '2' are the ID of impairment group.
		"""

		GroupTLP = defaultdict(list)
		SortedLoanPortfolio = sorted(self.LoanPortfolio, key=lambda x: str(x[1]))

		for row in SortedLoanPortfolio :
			Amount = float(row[2]) * float(ImpairmentCalculator._ReportingRate[str(row[3])])
			GroupTLP[str(row[0])].append({"MonthEndDate": str(row[1]),
										  "OutstandingAmount": Amount,
										  "Currency": ImpairmentCalculator._BaseCurrency,
										  "Branch": str(row[3])
										  })

		return dict(GroupTLP)


	def getPARAgingByGroup(self):
		"""
			PAR = Portfolio At Risk
			Data to be return sample:
			------------------------------------------------------------------------
			{'1': [
					{'Aging2DueAmt': 30026.0, 'Aging3DueAmt': 13976.0, 'Aging4DueAmt': 36281.0, 'Aging1DueAmt': 108959.0, 'MonthEndDate': '2016-01-31'},
					{'Aging2DueAmt': 39768.0, 'Aging3DueAmt': 18242.0, 'Aging4DueAmt': 32449.0, 'Aging1DueAmt': 125007.0, 'MonthEndDate': '2016-02-29'},
					{'Aging2DueAmt': 27800.0, 'Aging3DueAmt': 18885.0, 'Aging4DueAmt': 30575.0, 'Aging1DueAmt': 76684.5, 'MonthEndDate': '2016-03-31'},
					{'Aging2DueAmt': 23220.0, 'Aging3DueAmt': 15821.0, 'Aging4DueAmt': 30646.0, 'Aging1DueAmt': 78818.0, 'MonthEndDate': '2016-04-30'},
					{'Aging2DueAmt': 30600.0, 'Aging3DueAmt': 10387.0, 'Aging4DueAmt': 28119.0, 'Aging1DueAmt': 71728.0, 'MonthEndDate': '2016-05-31'},
					{'Aging2DueAmt': 30101.0, 'Aging3DueAmt': 17532.0, 'Aging4DueAmt': 21243.0, 'Aging1DueAmt': 54016.0, 'MonthEndDate': '2016-06-30'},
					{'Aging2DueAmt': 24194.0, 'Aging3DueAmt': 17275.0, 'Aging4DueAmt': 20528.0, 'Aging1DueAmt': 61344.0, 'MonthEndDate': '2016-07-31'},
					...
					],
			...
			}
			'1' is the impairment group ID.
		"""
		PARAgingGroup = defaultdict(list)
		PARPeriodLength = len(getAgingSetting()) # getAgingSetting() means IFRS Setting on field PAR Period
		AgingDueAmtDict = {}
		Dict = {}

		for row in self.PARAging :
			Currency = str(row[3])
			Rate = float(ImpairmentCalculator._ReportingRate.get(Currency))

			Dict = {
					"MonthEndDate": str(row[2]),
					"Currency": ImpairmentCalculator._BaseCurrency,
					"Branch": str(row[0])
					}

			for i in range(1, PARPeriodLength+1) :
				AgingDueAmtDict.update({"Aging%sDueAmt"%i : float(row[3+i]) * Rate})

			Dict.update(AgingDueAmtDict)

			PARAgingGroup[str(row[1])].append(Dict)
			Dict = {}

		return dict(PARAgingGroup)


	def getPARAgingDict(self):
		PARAging = defaultdict(list)

		for Key, Value in self.getTLPByGroup().items() :
			# Key = Impairment Group ID
			for LoanPortfolio in Value :
				# Filter function: lamda x means object of function getPARAgingByGroup
				PARAgingRow = filter(lambda x: x.get("MonthEndDate","")==LoanPortfolio.get("MonthEndDate",""),
									self.getPARAgingByGroup().get(Key)
									)[0]

				LoanPortfolio.update(PARAgingRow)
				PARAging[Key].append(LoanPortfolio)

		return dict(PARAging)


	def getRiskPortfolio(self):

		RiskPortfolioList = []

		for Row in self.RiskPortfolio :
			RiskPortfolioList.append({
				"Branch": str(Row[0]),
				"LoanID": str(Row[1]),
				"ImpairmentGroup": str(Row[2]),
				"Currency": str(Row[3]),
				"OutstandingAmount": float(Row[4]),
				"NumDayDue": int(mktpdcollection.getNumDayDue(str(Row[1])))
				})

			print "#######################", Row

		return RiskPortfolioList


	def getPDGroup(self):
		PDGroup = defaultdict(list)

		for Key, Value in self.getPARAgingDict().items() :
			PD1 = 0
			PD2 = 0
			PD3 = 0
			PD4 = 0
			PD5 = 0
			PD6 = 0

			for index, LoanAging in enumerate(Value) :
				PDGroup[Key].append({
					"Branch": LoanAging.get("Branch"),
					"MonthEndDate": LoanAging.get("MonthEndDate"),
					"PD1": 1 if PD1 > 1 else PD1,
					"PD2": 1 if PD2 > 1 else PD2,
					"PD3": 1 if PD3 > 1 else PD3,
					"PD4": 1 if PD4 > 1 else PD4,
					"PD5": 1 if PD5 > 1 else PD5,
					"PD6": 1 if PD6 > 1 else PD6
				})

				if index != len(Value) - 1 :
					Aging1DueAmt = LoanAging.get("Aging1DueAmt", 0)
					Aging2DueAmt = LoanAging.get("Aging2DueAmt", 0)
					Aging3DueAmt = LoanAging.get("Aging3DueAmt", 0)
					Aging4DueAmt = LoanAging.get("Aging4DueAmt", 0)
					Aging5DueAmt = LoanAging.get("Aging5DueAmt", 0)
					Aging6DueAmt = LoanAging.get("Aging6DueAmt", 0)

					Aging1DueAmtNext = float(Value[index+1].get("Aging1DueAmt", 0))
					Aging2DueAmtNext = float(Value[index+1].get("Aging2DueAmt", 0))
					Aging3DueAmtNext = float(Value[index+1].get("Aging3DueAmt", 0))
					Aging4DueAmtNext = float(Value[index+1].get("Aging4DueAmt", 0))
					Aging5DueAmtNext = float(Value[index+1].get("Aging5DueAmt", 0))
					Aging6DueAmtNext = float(Value[index+1].get("Aging6DueAmt", 0))

					PD1 = Aging1DueAmtNext/LoanAging.get("OutstandingAmount", 0) if Aging1DueAmt else 0
					PD2 = Aging2DueAmtNext/Aging1DueAmt if Aging1DueAmt else 0
					PD3 = Aging3DueAmtNext/Aging2DueAmt if Aging2DueAmt else 0
					PD4 = Aging4DueAmtNext/Aging3DueAmt if Aging3DueAmt else 0
					PD5 = Aging5DueAmtNext/Aging4DueAmt if Aging4DueAmt else 0
					PD6 = Aging6DueAmtNext/Aging5DueAmt if Aging5DueAmt else 0

		return dict(PDGroup)


	def getAveragePDGroup(self):
		"""
			PD = Probability of Default (%)
		"""
		AveragePDGroup = defaultdict(dict)

		for Key, Value in self.getPDGroup().items() :
			AvgPD1 = np.average([PD.get("PD1") for PD in Value if PD.get("PD1",0)>0 ])
			AvgPD2 = np.average([PD.get("PD2") for PD in Value if PD.get("PD2",0)>0 ])
			AvgPD3 = np.average([PD.get("PD3") for PD in Value if PD.get("PD3",0)>0 ])
			AvgPD4 = np.average([PD.get("PD4") for PD in Value if PD.get("PD4",0)>0 ])
			AvgPD5 = np.average([PD.get("PD5") for PD in Value if PD.get("PD5",0)>0 ])
			AvgPD6 = np.average([PD.get("PD6") for PD in Value if PD.get("PD6",0)>0 ])

			AveragePDGroup[Key].update({
				"Branch": Value[0].get("Branch"),
				"PD1": AvgPD1 if not np.isnan(AvgPD1) else 0,
				"PD2": AvgPD2 if not np.isnan(AvgPD2) else 0,
				"PD3": AvgPD3 if not np.isnan(AvgPD3) else 0,
				"PD4": AvgPD4 if not np.isnan(AvgPD4) else 0,
				"PD5": AvgPD5 if not np.isnan(AvgPD5) else 0,
				"PD6": AvgPD6 if not np.isnan(AvgPD6) else 0
				})

		return dict(AveragePDGroup)


	def getPDProduct(self):

		PDProductGroup = defaultdict(dict)
		PARPeriodLength = len(getAgingSetting())
		PDDict = {}

		for Key, Value in self.getAveragePDGroup().items() :
			ValueCopy = deepcopy(Value)
			ValueCopy.pop("Branch")

			for i in range(0, PARPeriodLength+1):
				PDDict.update({
					"PD" + str(i): np.prod([Value.get("PD" + str(j)) for j in range(i+1, PARPeriodLength+1)])
				})
			PDDict.update({
				"PD" + str(PARPeriodLength): PDDict.get("PD" + str(PARPeriodLength-1))
				})

			PDProductGroup[Key].update({
				"Branch": Value.get("Branch"),
				"CurrentPD": PDDict.get("PD0")
				})
			PDDict.pop("PD0")
			PDProductGroup[Key].update(PDDict)

		return dict(PDProductGroup)


	def getHistoricalEAD(self):
		"""
			EAD = Exposure At Default (Loan Outstanding Amount)
			This method is to get EAD from historical data
		"""
		EADDict = defaultdict(dict)
		Dict = {}

		for Key, Value in self.getPARAgingDict().items() :
			SortedValue = sorted(Value, key=lambda x: x.get("MonthEndDate"))
			LatestPARAging = SortedValue[-1]
			PARAgingCopy = deepcopy(LatestPARAging)
			PARAgingCopy.pop("MonthEndDate")
			PARAgingCopy.pop("OutstandingAmount")
			PARAgingCopy.pop("Currency")
			PARAgingCopy.pop("Branch")

			TotalDueAmt = sum([Val for Val in PARAgingCopy.values()])
			CurrentDueAmt = LatestPARAging.get("OutstandingAmount") - TotalDueAmt

			Dict = {
				"CurrentDueAmt": CurrentDueAmt,
				"Currency": str(LatestPARAging.get("Currency")),
				"Branch": str(LatestPARAging.get("Branch"))
			}

			Dict.update(PARAgingCopy)
			EADDict[Key].update(Dict)
			Dict = {}

		return dict(EADDict)


	def getEAD(self):
		"""
			EAD = Exposure At Default (Loan Outstanding Amount)
		"""
		EADDict = defaultdict(dict)
		AgingDict = defaultdict(list)
		Dict = {}

		for Row in self.getRiskPortfolio() :
			AgingGroup = getAgingByNumDay(Row.get("NumDayDue",0))
			GroupID = Row.get("ImpairmentGroup","")
			AgingDict[GroupID + "," + AgingGroup + "," + str(Row.get("Currency"))].append(
				Row.get("OutstandingAmount", 0)
				)

			Dict = {
				"Currency": str(Row.get("Currency")),
				"Branch": str(Row.get("Branch"))
			}

			EADDict[Row.get("ImpairmentGroup", "")].update(Dict)
			Dict = {}

		print("===========", AgingDict, dict(EADDict))

		for Key, Value in dict(EADDict).items():
			for K, V in dict(AgingDict).items():

				if Key in str(K) :
					print("*******", Key, V, Value)
					EADDict[Key].update({
					 	str(K).split(",")[1] : sum(map(float,V))
					 	})

		print("----====----", dict(EADDict))

		return dict(EADDict)


	def getPARCollection(self):
		PARCollectionDict = defaultdict(list)

		for Row in self.PARCollection :
			Branch = str(Row[9])
			Currency = str(Row[7])
			Rate = float(ImpairmentCalculator._ReportingRate.get(Currency))
			EIR = float(Row[6])
			MonthEndDate = str(Row[2])
			DateOfDefault = str(Row[3])
			DateDiff = date(*map(int, MonthEndDate.split("-"))) - date(*map(int, DateOfDefault.split("-")))
			NumOfDay = DateDiff.days if DateDiff.days > 0 else 0
			CollectedAmount = float(Row[4]) * Rate
			OutstandingAmount = float(Row[5]) * Rate
			CollectedAmountNPV = CollectedAmount*((1+EIR)**-(float(NumOfDay)/365)) * Rate

			# print "===", str(Row[0]), NumOfDay, CollectedAmount, OutstandingAmount, CollectedAmountNPV, EIR

			PARCollectionDict[str(Row[0])].append({
				"MonthEndDate": MonthEndDate,
				"DateOfDefault": DateOfDefault,
				"CollectedAmount": CollectedAmount,
				"CollectedAmountNPV": CollectedAmountNPV,
				"OutstandingAmount": OutstandingAmount,
				"EIR": EIR,
				"Currency": ImpairmentCalculator._BaseCurrency,
				"Branch": Branch
				})

		return dict(PARCollectionDict)


	def getLGDByLoan(self):
		"""
			LGD = Loss Given Default (%)
			NPV of Loan Collection = CollectedAmountPerInstallment*((1+EIR)^-((MonthEndDate - DateOfDefault)/365)))
			LGD of each loan = SUM(NPV of Loan Collection Each Installment)/Outstanding
			TotalLGD = np.average(list(LGD of each loan))
		"""
		LGD = 0
		LGDList = []

		for Key, Value in self.getPARCollection().items() :
			TotalCollected = sum([Val.get("CollectedAmount", 0) for Val in Value])
			TotalCollectedNPV = sum([Val.get("CollectedAmountNPV",0) for Val in Value])
			OutstandingAmount = min([Val.get("OutstandingAmount", 0) for Val in Value])
			DateOfDefault = min([Val.get("DateOfDefault", "") for Val in Value])
			EIR = Value[0].get("EIR",0)
			Currency = Value[0].get("Currency",0)
			Branch = Value[0].get("Branch", "")
			CollectedRatio = TotalCollectedNPV/OutstandingAmount

			if CollectedRatio < 0 : CollectedRatio = 0
			elif CollectedRatio > 1 : CollectedRatio = 1

			# print ":"*120, Branch, CollectedRatio

			LGD = 1 - CollectedRatio
			# print "-------", LGD
			LGDList.append({
				"Branch": Branch,
				"LoanID": Key,
				"LGD": LGD,
				"Currency": Currency,
				"TotalCollected": TotalCollected,
				"TotalCollectedNPV": TotalCollectedNPV,
				"OutstandingAmount": OutstandingAmount,
				"EIR": EIR,
				"DateOfDefault": DateOfDefault,
				"ImpairmentGroup": str(Key)
				})

		return LGDList


	def getAverageLGD(self):

		AvgLGD = np.average([Val.get("LGD", 0) for Val in self.getLGDByLoan()])

		return round(AvgLGD, 4)


	def getGroupImpairedAmt(self):
		"""

		"""
		ImpAmtGroup = defaultdict(dict)
		PD = self.getPDProduct()
		LGD = self.getAverageLGD()
		EAD = self.getEAD()
		PARPeriodLength = len(getAgingSetting())
		ImpDict = {}

		for Key, Value in PD.items() :

			if EAD.get(Key) :
				CurrentImp = Value.get("CurrentPD") * LGD * EAD.get(Key, {}).get("CurrentDueAmt", 0)

				Branch = Value.get("Branch")

				Currency = EAD.get(Key).get("Currency")

				for i in range(1, PARPeriodLength+1):
					ImpDict.update({
						"Imp%s"%i : Value.get("PD%s"%i) * LGD * EAD.get(Key).get("Aging%sDueAmt"%i, 0)
					})

				ImpAmtGroup[Key].update({
					"Branch": Branch,
					"Currency": Currency,
					"CurrentImp": CurrentImp,
					})

				ImpAmtGroup[Key].update(ImpDict)

		return dict(ImpAmtGroup)


	def getImpairedAmtByLoan(self):
		"""

		"""
		LoanImp = {}
		PD = self.getPDProduct()
		LGD = self.getAverageLGD()
		EAD = self.getRiskPortfolio()
		ImpDict = {}
		ImpList = []

		for Key, Value in PD.items() :
			EADByLoan = filter(lambda x: str(x.get("ImpairmentGroup"))==str(Key), EAD)

			for Val in EADByLoan :
				LoanID = str(Val.get("LoanID"))
				PDID = "PD" + LoanID
				NumDayDue = int(mktpdcollection.getNumDayDue(LoanID)) # This is for real practice
				PD = Value.get(getPDByNumDay(NumDayDue))
				EADAmount = Val.get("OutstandingAmount", 0) # This is for real practice
				ImpairmentAmount = PD * LGD * EADAmount
				Branch = Value.get("Branch")
				Currency = Val.get("Currency")
				Rate = float(ImpairmentCalculator._ReportingRate.get(Currency))

				LoanImp.update({
					"Branch": Branch,
					"Currency": Currency,
					"LoanID": str(Val.get("LoanID")),
					"ImpairmentGroup": str(Key),
					"NumDayDue": int(NumDayDue),
					"PD": PD,
					"LGD": LGD,
					"LoanOutstanding": EADAmount,
					"ImpairmentAmount": ImpairmentAmount,
					"LCYImpairmentAmount": ImpairmentAmount * Rate
					})

				ImpList.append(LoanImp)
				LoanImp = {}

		return ImpList


	def getTotalImpairedGroup(self):

		ImpairmentDict = {}
		ImpairmentList = []

		for Key, Value in self.getGroupImpairedAmt().items() :
			Branch = Value.get("Branch")
			Currency = Value.get("Currency")
			Value.pop("Currency")
			Value.pop("Branch")
			GroupObj = MKT_IMP_GROUP.query.get(Key)
			GroupDesc = str(GroupObj.Description) if GroupObj else ""
			TotalImpAmt = sum(Value.values())
			ImpairmentDict.update({
				"Branch": Branch,
				"Currency": Currency,
				"GroupID": Key,
				"GroupDesc": GroupDesc,
				"ImpairmentAmount": TotalImpAmt
				})
			ImpairmentList.append(ImpairmentDict)
			ImpairmentDict = {}
		return ImpairmentList


def getLoanPortfolio(Branch="", GroupList=[]):
	LoanPortfolioObj = db.session.query(MKT_IMP_LOAN_PORTFOLIO.ImpairmentGroup,
							MKT_IMP_LOAN_PORTFOLIO.MonthEndDate,
							func.sum(MKT_IMP_LOAN_PORTFOLIO.OutstandingAmount).label("TotalOutstanding"),
							MKT_IMP_LOAN_PORTFOLIO.Currency,
							MKT_IMP_LOAN_PORTFOLIO.Branch
							).\
							filter(
								MKT_IMP_LOAN_PORTFOLIO.Branch == Branch,
								MKT_IMP_LOAN_PORTFOLIO.ImpairmentGroup.in_(GroupList)
								).\
							group_by(
								MKT_IMP_LOAN_PORTFOLIO.MonthEndDate,
								MKT_IMP_LOAN_PORTFOLIO.ImpairmentGroup,
								MKT_IMP_LOAN_PORTFOLIO.Currency,
								MKT_IMP_LOAN_PORTFOLIO.Branch
								).\
							order_by(MKT_IMP_LOAN_PORTFOLIO.MonthEndDate)

	return LoanPortfolioObj


def getPARAging(Branch="", GroupList=[]):
	AgingDict = getAgingSetting()
	QueryList = []

	for index, Value in enumerate(AgingDict) :
		Value = Value.replace("<=", "1>").replace(">=", "")
		Aging = Value.split(">")

		if len(Aging) == 2 :

			QueryList.append(
				func.sum(case(
					[(
						MKT_IMP_RISK_PORTFOLIO_AGING.NumDayDue.in_(range(int(Aging[0]), int(Aging[1])+1)),
						MKT_IMP_RISK_PORTFOLIO_AGING.OutAmount
					)],
					else_=0)).label("SumAging" + str(index+1))
				)

		elif len(Aging) == 1 :
			QueryList.append(
				func.sum(case(
					[(
						MKT_IMP_RISK_PORTFOLIO_AGING.NumDayDue > int(Aging[0]),
						MKT_IMP_RISK_PORTFOLIO_AGING.OutAmount
					)],
					else_=0)).label("SumAging" + str(index+1))
				)

	PARAgingObj = db.session.query(
						MKT_IMP_RISK_PORTFOLIO_AGING.Branch,
						MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup,
						MKT_IMP_RISK_PORTFOLIO_AGING.MonthEndDate,
						MKT_IMP_RISK_PORTFOLIO_AGING.Currency,
						*QueryList
						).\
						filter(
							MKT_IMP_RISK_PORTFOLIO_AGING.Branch == Branch,
							MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup.in_(GroupList)
							).\
						group_by(
							MKT_IMP_RISK_PORTFOLIO_AGING.MonthEndDate,
							MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup,
							MKT_IMP_RISK_PORTFOLIO_AGING.Currency,
							MKT_IMP_RISK_PORTFOLIO_AGING.Branch
							).\
						order_by(MKT_IMP_RISK_PORTFOLIO_AGING.MonthEndDate)

	return PARAgingObj


# def getRiskPortfolio(Branch="", GroupList=[]):
# 	RiskPortfolio = db.session.query(
# 						MKT_IMP_RISK_PORTFOLIO_AGING.Branch,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.LoanID,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.MonthEndDate,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.Currency,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.OutAmount,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.OutPriAmount,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.OutIntAmount,
# 						MKT_IMP_RISK_PORTFOLIO_AGING.NumDayDue
# 						).\
# 						filter(
# 							MKT_IMP_RISK_PORTFOLIO_AGING.Branch == Branch,
# 							MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup.in_(GroupList)
# 							)

# 	return RiskPortfolio


def getPARCollection(Branch="", GroupList=[]):
	PARCollectionObj = db.session.query(
							MKT_IMP_PAR_COLLECTION.LoanID,
							MKT_IMP_PAR_COLLECTION.ImpairmentGroup,
							MKT_IMP_PAR_COLLECTION.MonthEndDate,
							MKT_IMP_PAR_COLLECTION.DateOfDefault,
							MKT_IMP_PAR_COLLECTION.CollectedAmount,
							MKT_IMP_PAR_COLLECTION.OutstandingAmount,
							MKT_IMP_PAR_COLLECTION.EIR,
							MKT_IMP_PAR_COLLECTION.Currency,
							MKT_LOAN_CONTRACT.OutstandingAmount,
							MKT_IMP_PAR_COLLECTION.Branch
						).\
						outerjoin(MKT_LOAN_CONTRACT, MKT_LOAN_CONTRACT.ID == MKT_IMP_PAR_COLLECTION.LoanID).\
						filter(
							MKT_IMP_PAR_COLLECTION.Branch == Branch,
							).\
						order_by(MKT_IMP_PAR_COLLECTION.MonthEndDate)
	return PARCollectionObj


def getLoanOutstanding(Branch="", GroupList=[]):
	LoanOutstandingObj = db.session.query(
							MKT_LOAN_CONTRACT.Branch,
							MKT_LOAN_CONTRACT.ID,
							MKT_LOAN_CONTRACT.ImpairmentGroup,
							MKT_LOAN_CONTRACT.Currency,
							MKT_LOAN_CONTRACT.OutstandingAmount,
						).\
						filter(
							MKT_LOAN_CONTRACT.Branch==Branch,
							MKT_LOAN_CONTRACT.ImpairmentGroup.in_(GroupList)
						)

	return LoanOutstandingObj


def getPercentage(Ratio=0.0, sign=False):

	Percentage = round(float(Ratio), 4) * 100
	return "%.2f"%Percentage + ("%" if sign else "")


# def getLoanOutstanding(LoanID):

# 	LoanObj = MKT_LOAN_CONTRACT.query.get(LoanID)
# 	OutstandingAmount = float(LoanObj.OutstandingAmount) if LoanObj else 0
# 	return OutstandingAmount


def insertEADSummary(GeneratingID="", PARAgingGroup={}):

	db.session.query(MKT_IMP_EAD_SUMMARY).\
	filter(MKT_IMP_EAD_SUMMARY.GeneratingID==GeneratingID).\
	delete(synchronize_session='fetch')

	for Key, Value in PARAgingGroup.items() :
		for Val in Value :

			Dict = {
				"Branch": str(Val.get("Branch", "")),
				"ImpairmentGroup": str(Key),
				"GeneratingID": GeneratingID,
				"Currency": str(Val.get("Currency")),
				"LoanPortfolio": float(Val.get("OutstandingAmount")),
				"MonthEndDate": str(Val.get("MonthEndDate")),
				"Aging1DueAmt": float(Val.get("Aging1DueAmt", 0)),
				"Aging2DueAmt": float(Val.get("Aging2DueAmt", 0)),
				"Aging3DueAmt": float(Val.get("Aging3DueAmt", 0)),
				"Aging4DueAmt": float(Val.get("Aging4DueAmt", 0)),
				"Aging5DueAmt": float(Val.get("Aging5DueAmt", 0)),
				"Aging6DueAmt": float(Val.get("Aging6DueAmt", 0))
			}
			Dict.update(getAuditrial())

			Obj = MKT_IMP_EAD_SUMMARY(**Dict)
			db.session.add(Obj)
			Dict = {}


def insertPDSummary(GeneratingID="", PDGroup={}, PDAverage={}):

	db.session.query(MKT_IMP_PD_SUMMARY).\
	filter(MKT_IMP_PD_SUMMARY.GeneratingID==GeneratingID).\
	delete(synchronize_session='fetch')

	for Key, Value in PDGroup.items():
		for Val in Value :
			Dict = {
				"Branch": str(Value[0].get("Branch", "")),
				"ImpairmentGroup": str(Key),
				"GeneratingID": GeneratingID,
				"TypeOfData": "",
				"MonthEndDate": str(Val.get("MonthEndDate", "")),
				"PD1": float(Val.get("PD1",0)),
				"PD2": float(Val.get("PD2",0)),
				"PD3": float(Val.get("PD3",0)),
				"PD4": float(Val.get("PD4",0)),
				"PD5": float(Val.get("PD5",0)),
				"PD6": float(Val.get("PD6",0))
			}
			Dict.update(getAuditrial())
			Obj = MKT_IMP_PD_SUMMARY(**Dict)
			db.session.add(Obj)
			Dict = {}

	for Key, Value in PDAverage.items():
		Dict = {
			"Branch": str(Value.get("Branch", "")),
			"ImpairmentGroup": str(Key),
			"GeneratingID": GeneratingID,
			"MonthEndDate": str(Value.get("MonthEndDate", "")),
			"TypeOfData": "AVG",
			"PD1": float(Value.get("PD1",0)),
			"PD2": float(Value.get("PD2",0)),
			"PD3": float(Value.get("PD3",0)),
			"PD4": float(Value.get("PD4",0)),
			"PD5": float(Value.get("PD5",0)),
			"PD6": float(Value.get("PD6",0))
		}
		Dict.update(getAuditrial())
		Obj = MKT_IMP_PD_SUMMARY(**Dict)
		db.session.add(Obj)
		Dict = {}


def insertLGDSummary(GeneratingID="", LGDByLoan=[], LGDAverage=""):

	Branch = ""

	db.session.query(MKT_IMP_LGD_AVERAGE).\
	filter(MKT_IMP_LGD_AVERAGE.GeneratingID==GeneratingID).\
	delete(synchronize_session='fetch')

	for Val in LGDByLoan :
		Branch = str(Val.get("Branch", ""))
		Dict = {
			"Branch": Branch,
			"ImpairmentGroup": "",
			"GeneratingID": GeneratingID,
			"DateOfDefault": str(Val.get("DateOfDefault")),
			"Currency": str(Val.get("Currency")),
			"OutstandingAmount": float(Val.get("OutstandingAmount", 0)),
			"CollectedAmount": float(Val.get("TotalCollected", 0)),
			"CollectedAmountNPV": float(Val.get("TotalCollectedNPV", 0)),
			"LGD": str(Val.get("LGD")),
			"EIR": str(Val.get("EIR")),
			"LoanID": str(Val.get("LoanID"))
		}
		Dict.update(getAuditrial())

		Obj = MKT_IMP_LGD_SUMMARY(**Dict)
		db.session.add(Obj)
		Dict = {}

	Dict = {
		"Branch": Branch,
		"GeneratingID": GeneratingID,
		"LGD": LGDAverage
	}
	Dict.update(getAuditrial())

	Obj = MKT_IMP_LGD_AVERAGE(**Dict)
	db.session.add(Obj)
	Dict = {}


def insertImpCalculation(GeneratingID="",
		ImpGroupList=[],
		PDAverage={},
		PDProductGroup={},
		LGDAverage="",
		EAD={},
		GroupImpAmt={}
		):

	PARPeriodLength = len(getAgingSetting())
	PDDict = {}

	db.session.query(MKT_IMP_CALCULATION).\
	filter(MKT_IMP_CALCULATION.GeneratingID==GeneratingID).\
	delete(synchronize_session='fetch')

	for ImpGroup in ImpGroupList :

		for i in range(0, PARPeriodLength+1):
			if EAD.get(ImpGroup):
				ImpAmount = GroupImpAmt.get(ImpGroup)
				ImpAmt = ImpAmount.get("CurrentImp") if i==0 else ImpAmount.get("Imp" + str(i))
				Currency = ImpAmount.get("Currency")
				PD = PDAverage.get(ImpGroup)
				PDProduct = PDProductGroup.get(ImpGroup)

				PDDict = {"PD"+str(j): getPercentage(PD.get("PD"+str(j), "")) for j in range(i+1,PARPeriodLength+1)}

				if not PDDict :
					PDDict = {
						"PD" + str(PARPeriodLength): getPercentage(PD.get("PD"+str(PARPeriodLength), 0))
					}
				Dict = {
					"Branch": PDProduct.get("Branch", ""),
					"Currency": Currency,
					"ImpairmentGroup": ImpGroup,
					"GeneratingID": GeneratingID,
					"PD": getPercentage(PDProduct.get("CurrentPD", 0) if i==0 else PDProduct.get("PD" + str(i), 0)),
					"LGD": getPercentage(LGDAverage),
					"EAD": float(EAD.get(ImpGroup).get("CurrentDueAmt", 0) if i==0 else EAD.get(ImpGroup).get("Aging" + str(i) + "DueAmt", 0)),
					"ImpairmentAmount": float(ImpAmt)
				}
				Dict.update(PDDict)
				Dict.update(getAuditrial())

				Obj = MKT_IMP_CALCULATION(**Dict)
				db.session.add(Obj)

				Dict = {}


def insertImpByLoan(GeneratingID="" ,ImpList=[]):

	Audit = getAuditrial()

	db.session.query(MKT_IMP_LOAN_OUTSTANDING).\
	filter(MKT_IMP_LOAN_OUTSTANDING.GeneratingID==GeneratingID).\
	delete(synchronize_session='fetch')

	for Row in ImpList :
		Row.update(Audit)
		Row.update({"GeneratingID": GeneratingID})
		Obj = MKT_IMP_LOAN_OUTSTANDING(**Row)

		db.session.add(Obj)


def insertImpairmentData(GeneratingID="", Branch="", ImpairmentGroup=[]) :

	try :
		# Data Query
		LoanPortfolioObj 		= getLoanPortfolio(Branch, ImpairmentGroup)
		PARAgingObj 				= getPARAging(Branch, ImpairmentGroup)
		PARCollection 			= getPARCollection(Branch, ImpairmentGroup)
		LoanOutstandingObj 	= getLoanOutstanding(Branch, ImpairmentGroup)

		ImpCalculator 			= ImpairmentCalculator(
														LoanPortfolioObj,
														PARAgingObj,
														PARCollection,
														LoanOutstandingObj
													)

		# Data Manipulate
		# Step to Calculate Summary PD
		PARAgingGroup 	= ImpCalculator.getPARAgingDict() # Generate Aging Loan Portfolio at Risk Amount
		PDGroup 		= ImpCalculator.getPDGroup() # Generate PD Percentage by Group (PD = Probability of Default)
		PDAverage 		= ImpCalculator.getAveragePDGroup() # Generate PD Average by Group
		# Step to Calculate LGD
		PARCollection 	= ImpCalculator.getPARCollection()
		LGDByLoan 		= ImpCalculator.getLGDByLoan()
		LGDAverage 		= ImpCalculator.getAverageLGD()
		EAD 			= ImpCalculator.getEAD()
		PDProduct 		= ImpCalculator.getPDProduct()
		TotImp 			= ImpCalculator.getTotalImpairedGroup()
		GroupImpAmt 	= ImpCalculator.getGroupImpairedAmt()

		# Data Insert
		insertEADSummary(GeneratingID, PARAgingGroup)
		insertPDSummary(GeneratingID, PDGroup, PDAverage)
		insertLGDSummary(GeneratingID, LGDByLoan, LGDAverage)
		ImpGroupList = PARAgingGroup.keys()
		insertImpCalculation(GeneratingID,
			ImpGroupList,
			PDAverage,
			PDProduct,
			LGDAverage,
			EAD,
			GroupImpAmt)

		ImpLoan = ImpCalculator.getImpairedAmtByLoan()
		print "++++++", ImpLoan
		insertImpByLoan(GeneratingID, ImpLoan)

		print "#"*120, GeneratingID, ImpairmentGroup, Branch, ImpLoan

		db.session.commit()

	except Exception as e :
		print e

		db.session.rollback()

# insertImpairmentData("1111", "HO", ["1", "2", "3"])

# print "jjjjjjj"

# LoanPortfolioObj = getLoanPortfolio()
# PARAgingObj 	= getPARAging()
# PARCollection 	= getPARCollection()
# RiskPortfolio = getRiskPortfolio()
# ImpCalculator 	= ImpairmentCalculator(LoanPortfolioObj, PARAgingObj, PARCollection, RiskPortfolio)
# ImpLoan = ImpCalculator.getImpairedAmtByLoan()

# insertImpByLoan(ImpLoan)
# db.session.commit()

# def getTotalImp():

# 	LoanImpairedObj = MKT_LOAN_IMPAIRMENT.query.filter(MKT_LOAN_IMPAIRMENT.ImpairmentGroup=='3').all()
# 	Amount = sum([float(Imp.ImpairmentAmount) for Imp in LoanImpairedObj])

# 	return Amount

# def getTotalRiskPortfolio():

# 	LoanImpairedObj = MKT_IMP_RISK_PORTFOLIO_AGING.query.filter(MKT_IMP_RISK_PORTFOLIO_AGING.ImpairmentGroup=='3').all()
# 	Amount = sum([float(Imp.OutAmount) for Imp in LoanImpairedObj])

# 	return Amount

# print "===========", getTotalImp()
# print "-----------", getTotalRiskPortfolio()