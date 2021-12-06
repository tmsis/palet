from Enrollment import Enrollment
from Trend import Trend

t = Trend().getMonthOverMonth()
e = Enrollment(t)
e.sql()


# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')