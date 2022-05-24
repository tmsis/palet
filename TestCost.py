from palet.Cost import Cost
from palet.Enrollment import Enrollment

cost = Cost.inpatient()

api = Enrollment().byState(['NY']).byMonth().calculate(cost)
# api = Enrollment().byMonth().calculate(cost)

print(api.sql())
