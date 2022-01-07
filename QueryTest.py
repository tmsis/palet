# This is a class to test functionality from outside the packages.
# use it as you will.
# TODO: Refactor to have automated testing for breakage

from palet.Trend import Trend
from palet.Enrollment import Enrollment
from palet.Palet import Palet

t = Trend().byMonth()
# e = Enrollment(t).byAgeRange('0-18,65+').byState(State.state_fips).byIncomeBracket('01')
e = Enrollment(t).byIncomeBracket('01')
print(e.sql())
e.fetch()

Palet.Utils.propertiesOf(e)

e = Enrollment()
print(e.by_group)

# print(StateHelper.displayValues())
# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')
