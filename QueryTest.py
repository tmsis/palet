
from palet.AgeGroupHelper import AgeGroupHelper
from palet.StateHelper import StateHelper
from palet.Trend import Trend
from palet.Enrollment import Enrollment
from palet.Palet import Palet

AgeGroup = AgeGroupHelper()
State = StateHelper()
t = Trend().getMonthOverMonth()
e = Enrollment(t).byAgeRange(AgeGroup.Child).byState(State.NY).byIncomeBracket()
print(e.sql())

Palet.Utils.propertiesOf(e)

#print(StateHelper.displayValues())
# .byState('37').byEthnicity('01').byAgeRange('18-21').byGender('F')