class Readmit:

    by = {}
    by_group = []
    filter = {}

    analyses = []

    def enrollmentIntervals(self):
        return self

    def byEthnicity(self):
        # print('by ethnicity...')
        self.by_group.append('ethnicity')
        return self

    def byState(self):
        # print('by state...')
        self.by_group.append('state')
        return self

    def ages(self, band):
        return self

    def __init__(self) -> None:

        self.by = {
            'ethnicity': self.byEthnicity,
            'state': self.byState,
        }

        self.analyses = {
        }

    def fetch(self):

        sql = f"""
                select
                    {','.join(self.by_group)}
                    , count(distinct blah) as blahs
                from
                    foo
                where

                group by
                    {','.join(self.by_group)}
            """

        return sql

print(Readmit().fetch())
print(Readmit().byState().fetch())
print(Readmit().byState().byEthnicity().fetch())
print(Readmit().byEthnicity().byState().fetch())
