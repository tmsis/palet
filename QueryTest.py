# This is a class to test functionality from outside the packages.
# use it as you will.
# TODO: Refactor to have automated testing for breakage

from palet.Enrollment import Enrollment


# print(PaletMetadata.Enrollment.raceEthnicity.race_ethncty_flag[1])
# # t = Trend().byMonth()
# # e = Enrollment().byAgeRange('0-18,65+')
# # e = Enrollment(t).byIncomeBracket('01').byState(State.state_fips)
# # e = Enrollment(t).byMonth(PaletMetadata.Enrollment.CHIP.monthly.Dec).byState(State.state_fips)
# # e = Enrollment().byGender('M').byRaceEthnicity('1')
# # e = Enrollment().byYear()
# e = Eligibility().byMonth()
# print(e.sql())
# e.fetch()

# Palet.Utils.propertiesOf(e)

# e = Enrollment()
# print(e.by_group)

# print(StateHelper.displayValues())
# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')
api = Enrollment()
api = Enrollment().byCoverageType(['01']).byYear(['2019']).byMonth(['04'])
print(api.sql())
