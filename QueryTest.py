# This is a class to test functionality from outside the packages.
# use it as you will.
# TODO: Refactor to have automated testing for breakage

from palet.Eligibility import Eligibility
from palet.Trend import Trend
from palet.Enrollment import Enrollment
# from palet.State import State
from palet.Palet import Palet
from palet.PaletMetadata import PaletMetadata

print(PaletMetadata.Enrollment.raceEthnicity.race_ethncty_flag[1])
# t = Trend().byMonth()
# e = Enrollment().byAgeRange('0-18,65+')
# e = Enrollment(t).byIncomeBracket('01').byState(State.state_fips)
# e = Enrollment(t).byMonth(PaletMetadata.Enrollment.CHIP.monthly.Dec).byState(State.state_fips)
# e = Enrollment().byGender('M').byRaceEthnicity('1')
e = Eligibility()
print(e.sql())
e.fetch()

Palet.Utils.propertiesOf(e)

e = Enrollment()
print(e.by_group)

# print(StateHelper.displayValues())
# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')
