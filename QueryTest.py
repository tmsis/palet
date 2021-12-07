
from palet.Trend import Trend
from palet.Enrollment import Enrollment

t = Trend().getMonthOverMonth()
e = Enrollment(t).byAgeRange('18-21')
print(e.sql())


# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')