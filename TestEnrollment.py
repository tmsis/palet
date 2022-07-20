from palet.Cost import Cost
from palet.Enrollment import Enrollment

cost = Cost.inpatient()

api = Enrollment('full').byState(['NY']).byMonth().calculate(cost)

api.timeunit = 'full'
# api = Enrollment().byMonth().calculate(cost)

print(api.sql())
