# import sys
# sys.path.append('../')

# from palet.Diagnoses import Diagnoses
from palet.Readmits import Readmits
from palet.Enrollment import Enrollment
# from palet.ServiceCategory import ServiceCategory

# api = Enrollment().having(Readmits.allcause(30))
# api = Enrollment().byState(['NY']).having(Readmits.allcause(30))
api = Enrollment().byMonth().having(Readmits.allcause(30))

print(api.sql())
