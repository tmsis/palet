class Article():

  def __init__(self):
    self.by = {}
    self.by_group = []
    self.filter = {}
    self.where = []
    self.mon_group = []
    self.sql = ''


  def sql(self):
    print(self.sql)
