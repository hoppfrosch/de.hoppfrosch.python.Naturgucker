import pyspatialite.dbapi2 as db
import logging
from logging.config import dictConfig
import pandas as pd
import os
import semver

version = semver.format_version(0, 1, 0, 'pre.1', 'build.1')

def get_or_create(session, model, **kwargs):
  '''
  Creates an object or returns the object if exists
  credit to Kevin @ StackOverflow
  from: http://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
  '''
  instance = session.query(model).filter_by(**kwargs).first()
  if not instance:
    instance = model(**kwargs)
    session.add(instance)

  return instance


class ngdb:
  'Class handling Spatialite-DB for NG-data'
  version = '0.1'

  def __init__(self, dbname="NGdata.sqlite", overwrite=1):
    self.dbname = dbname
    if (overwrite):
      import os
      if os.path.isfile(dbname):
        os.remove(dbname)
    # creating/connecting the test_db
    import pyspatialite.dbapi2 as db
    self.conn = db.connect(dbname)
    self.conn.isolation_level = None
    # creating a Cursor
    self.cur = self.conn.cursor()
    if (overwrite):
      self.cur.execute('PRAGMA user_version="'+ self.version + '"')
      self.cur.execute('PRAGMA foreign_keys=ON')
      # initializing Spatial MetaData
      self.cur.execute("BEGIN")
      self.cur.execute('SELECT InitSpatialMetadata()')
      self.cur.execute("COMMIT")
    self._generateTables(overwrite=overwrite)
    # ##########################
    from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, inspect
    from sqlalchemy.ext.automap import automap_base
    self.engine = create_engine("sqlite:///"+dbname)
    metadata = MetaData()
    metadata.reflect(self.engine)
    # we can then produce a set of mappings from this MetaData.
    self.tbl = automap_base(metadata=metadata)
    # calling prepare() just sets up mapped classes and relationships.
    self.tbl.prepare()
    # mapped classes are ready
    from sqlalchemy.orm import Session
    self.session = Session(self.engine)

  def __del__(self):
    class_name = self.__class__.__name__
    self.conn.close()

  def sqliteversion(self):
    # testing library versions
    rs = self.cur.execute('SELECT sqlite_version()')
    for row in rs:
      return row[0]
    rs.close()

  def spatialiteversion(self):
    # testing library versions
    rs = self.cur.execute('SELECT spatialite_version()')
    for row in rs:
      return row[0]
    rs.close()

  def _generateTables(self, overwrite = 1):
    if (overwrite):
      #### Tabelle Artengruppe ######
      self.cur.execute('DROP table IF EXISTS Artengruppe')
      sql = (
              "CREATE TABLE Artengruppe ("
                "artengruppe_pk INTEGER PRIMARY KEY AUTOINCREMENT,"
                "name TEXT UNIQUE NOT NULL,"
                "englisch TEXT,"
                "deutsch TEXT"
              ");"
            )
      self.cur.execute(sql)
      #### Tabelle Gattung ######
      self.cur.execute('DROP table IF EXISTS Gattung')
      sql = (
              "CREATE TABLE Gattung ("
                "gattung_pk INTEGER PRIMARY KEY AUTOINCREMENT,"
                "name TEXT UNIQUE NOT NULL,"
                "englisch TEXT,"
                "deutsch TEXT,"
                "fk_artengruppe REFERENCES Artengruppe(artengruppe_pk)"
              ")"
            )
      self.cur.execute(sql)
      #### Tabelle Art ######
      self.cur.execute('DROP table IF EXISTS Art')
      sql = (
              "CREATE TABLE Art ("
                "art_pk INTEGER PRIMARY KEY AUTOINCREMENT,"
                "name TEXT NOT NULL,"
                "englisch TEXT,"
                "deutsch TEXT,"
                "fk_gattung REFERENCES Gattung(gattung_pk),"
                "art_id INTEGER,"
                "tax_ordnr TEXT UNIQUE"
              ")"
            )
      self.cur.execute(sql)
      #### Tabelle Beobachtung ######
      self.cur.execute('DROP table IF EXISTS Beobachtung')
      sql = (
              "CREATE TABLE Art ("
                "beobachtung_pk  INTEGER PRIMARY KEY AUTOINCREMENT"
              ")"
            )
      self.cur.execute(sql)

  def get_or_create(self, model, **kwargs):
    instance = self.session.query(model).filter_by(**kwargs).first()
    if instance:
      return instance
    else:
      instance = model(**kwargs)
      self.session.add(instance)
      self.session.commit()
      return instance

########################################################################################################################
# Configure logging
logging_config = dict(
    version = 1,
    formatters = {
        'f': {'format':
              '%(asctime)s [%(levelname)-2s] [%(name)-2s] %(message)s'}
        },
    handlers = {
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.DEBUG}
        },
    root = {
        'handlers': ['h'],
        'level': logging.DEBUG,
        },
)

dictConfig(logging_config)

logger = logging.getLogger()

# Initialize coloredlogs.
#import coloredlogs
#coloredlogs.install(level='DEBUG')
logger.info("***** %s v%s *************************************",os.path.basename(__file__), version)

########################################################################################################################
x = ngdb()
# testing library versions
logger.info("Using SQLite v%s, Spatialite v%s",x.sqliteversion(), x.spatialiteversion())

########################################################################################################################
df = pd.read_csv('NGExport.csv', sep=';', encoding='utf-8')
for index, row in df.iterrows():
  artengruppe = x.get_or_create(x.tbl.classes.Artengruppe, name=row['Artengruppe'], deutsch=row['Artengruppe'])
  gattung = x.get_or_create(x.tbl.classes.Gattung, name=row['Gattung'], fk_artengruppe=artengruppe.artengruppe_pk)
  art = x.get_or_create(x.tbl.classes.Art, name=row['Art'], fk_gattung=gattung.gattung_pk,tax_ordnr=row['Taxonom. Ordnungsnr.'],art_id=row['ArtID'],deutsch=row['Trivialname'])
  logger.info("Processing line (%d/%d) - %s %s (%s)",index, len(df.index),row['Gattung'], row['Art'], row['Trivialname'])

########################################################################################################################
quit()
