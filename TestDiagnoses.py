from numpy import diag
from palet.Diagnoses import Diagnoses
from palet.Enrollment import Enrollment
from palet.ServiceCategory import ServiceCategory

# AFib = ['I230', 'I231', 'I232', 'I233', 'I234', 'I235', 'I236', 'I237', 'I238', 'I213', 'I214', 'I219', 'I220', 'I221', 'I222', 'I228', 'I229', 'I21A1', 'I21A9', 'I2101', 'I2102', 'I2109', 'I2111', 'I2119', 'I2121', 'I2129']
AFib = ['I230']
Diabetes = ['E0800']

# api = Enrollment().having(Diagnoses.where(ServiceCategory.inpatient, AFib))

# for i in api.having_constraints:
#     print(i)

# api = Enrollment().mark(Diagnoses.where(ServiceCategory.inpatient, AFib), 'AFib')
# api = Enrollment().mark(                        \
#     Diagnoses.within([                          \
#         (ServiceCategory.inpatient, 1)],        \
#         AFib, 1), 'AFib')

# assert a == AFib
# print(api.sql())


afib = Diagnoses.within([
        (ServiceCategory.inpatient, 1),
        (ServiceCategory.other_services, 2)],
        AFib, 1)

diabetes = Diagnoses.within([
        (ServiceCategory.inpatient, 1),
        (ServiceCategory.other_services, 2)],
        Diabetes, 1)

# print(afib)
# print(diabetes)

# api = Enrollment().mark(afib, 'AFib')
# api = Enrollment().mark(Diagnoses.within([(ServiceCategory.inpatient, 1)], AFib, 2), 'AFib')
api = Enrollment().mark(afib, 'AFib').mark(diabetes, 'Diabetes')

print(api.sql())
