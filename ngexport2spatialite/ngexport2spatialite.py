import pyspatialite.dbapi2 as db
import logging
from logging.config import dictConfig
import pandas as pd
import os
import semver
import datetime

version = semver.format_version(0, 1, 0, 'pre.6', 'build.1')

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

########################################################################################################################
# Hilfsklasse fuer Beobachtungen
class beobachtung(object):
  def __init__(self, cur):
    self.cur = cur
    # do something
    return

  # Update der Beobachtungsgeometry
  def update_geometry(self, pk, lon, lat):
    sql = (
            "UPDATE beobachtung SET "
            "geom=GeomFromText('POINT(" + lon + " " + lat + ")', 4326)"
            "WHERE beobachtung_pk=" + pk
          )
    self.cur.execute(sql)
    return

########################################################################################################################
# Hilfsklasse fuer Gebiete
class gebiet(object):
  def __init__(self, cur):
    self.cur = cur
    # do something
    return

  # Update der Beobachtungsgeometry
  def update_geometry(self, pk, lon, lat):
    sql = (
            "UPDATE gebiet SET "
            "geom=GeomFromText('POINT(" + lon + " " + lat + ")', 4326)"
            "WHERE gebiet_pk=" + pk
          )
    self.cur.execute(sql)
    return

########################################################################################################################
# Hauptklasse
class ngdb(object):
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
    self.beobachtung = beobachtung(self.cur)
    self.gebiet = gebiet(self.cur)

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
              "CREATE TABLE Beobachtung ("
                "beobachtung_pk  TEXT PRIMARY KEY UNIQUE,"
                "fk_art REFERENCES Art(art_pk),"
                "gebietskoordinaten INTEGER,"
                "fk_gebiet REFERENCES Gebiet(gebiet_pk),"
                "datum_start TEXT,"
                "datum_ende TEXT,"
                "anzahl_quantifier TEXT,"
                "anzahl INTEGER"
              ")"
            )
      self.cur.execute(sql)
      # creating a POINT Geometry column
      sql = "SELECT AddGeometryColumn('Beobachtung', 'geom', 4326, 'POINT', 'XY')"
      self.cur.execute(sql)
      #### Tabelle Gebiet ######
      self.cur.execute('DROP table IF EXISTS Gebiet')
      sql = (
              "CREATE TABLE Gebiet ("
                "gebiet_pk  INTEGER PRIMARY KEY AUTOINCREMENT,"
                "gebietsname TEXT NOT NULL,"
                "land TEXT NOT NULL,"
                "provinz TEXT,"
                "autokennzeichen TEXT"
              ")"
            )
      self.cur.execute(sql)
      # creating a POINT Geometry column
      sql = "SELECT AddGeometryColumn('Gebiet', 'geom', 4326, 'POINT', 'XY')"
      self.cur.execute(sql)
      #### View Summary##################
      #### View zur Verwendung in QGIS, da einfach darauf gefiltert werden kann!!! #####################################
      self.cur.execute('DROP view IF EXISTS vwSummary')
      sql = (
              "CREATE VIEW vwSummary AS "
                "SELECT "
                "  Art.deutsch as artname,"
                "  Beobachtung.datum_start as von,"
                "  Beobachtung.datum_ende as bis,"
                "  Gebiet.gebietsname,"
                "  Gebiet.land,"
                "  Gebiet.provinz,"
                "  Gebiet.autokennzeichen,"
                "  Beobachtung.gebietskoordinaten,"
                "  Beobachtung.geom as geom "
                "FROM Art, Beobachtung, Gebiet "
                "WHERE "
                "Art.art_pk=Beobachtung.fk_art "
                "AND Beobachtung.fk_gebiet=Gebiet.gebiet_pk"
            )
      self.cur.execute(sql)
      # Damit in dem View die Geometrie-Daten verwendet werden koennen, muss man dies noch in der internen
      # Verwaltungstabelle "views_geometry_columns" registrieren...
      # Quelle: http://www.adventurer.org.nz/?page=gis/QGIS_tutorials/500_Databases/524_Creating_spatial_views.txt
      sql = (
                "INSERT INTO views_geometry_columns "
                   "(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column, read_only) "
                "VALUES"
                  "('vwsummary', 'geom', 'rowid', 'beobachtung', 'geom', 1)"
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

  if pd.isnull(row['Trivialname']):
    artname = row['Gattung'] + " " + row['Art']
  else:
    artname = row['Trivialname']

  art = x.get_or_create(x.tbl.classes.Art, name=row['Art'], fk_gattung=gattung.gattung_pk,tax_ordnr=row['Taxonom. Ordnungsnr.'],art_id=row['ArtID'],deutsch=artname)

  # Tabelle Gebiet
  gebiet = x.get_or_create(x.tbl.classes.Gebiet, gebietsname=row['Gebietsname'], land=row['Land'],provinz=row['Provinz'],autokennzeichen=row['Autokennzeichen'])
  lon = row['Koordinate E']
  lat = row['Koordinate N']
  lon = lon.replace(',','.')
  lat = lat.replace(',','.')
  ret = x.gebiet.update_geometry(str(gebiet.gebiet_pk), lon, lat)


  # Tabelle Beobachtung
  lon = row['Punktverortung E']
  lat = row['Punktverortung N']
  bbereich = 0
  if (lat == "0,00000") and (lon == "0,00000"):
    lon = row['Koordinate E']
    lat = row['Koordinate N']
    bbereich = 1
  lon = lon.replace(',','.')
  lat = lat.replace(',','.')

  sStart=row['Datum']
  if pd.isnull(row['Uhrzeit_von']):
    sStart=row['Datum']+ " 0:00"
  else:
    sStart=row['Datum'] + " " + str(row['Uhrzeit_von'])
  sEnde=row['Datum']
  if pd.isnull(row['Uhrzeit_bis']):
    sEnde=row['Datum']+ " 23:59"
  else:
    sEnde=row['Datum'] + " " + str(row['Uhrzeit_bis'])

  if pd.isnull(row['Anzahl']):
    anz = -1
  else:
    anz = row['Anzahl']

  if pd.isnull(row['+/-']):
    aq = ''
  else:
    aq =  row['+/-']

  dstart=datetime.datetime.strptime(sStart , '%d.%m.%Y %H:%M').strftime("%Y-%m-%d %H:%M:%S")
  dende =datetime.datetime.strptime(sEnde  , '%d.%m.%Y %H:%M').strftime("%Y-%m-%d %H:%M:%S")
  b = x.get_or_create(
      x.tbl.classes.Beobachtung,
      beobachtung_pk=str(row['DatensatzID']),
      gebietskoordinaten=bbereich,
      fk_art=art.art_pk,
      fk_gebiet=gebiet.gebiet_pk,
      datum_start=dstart,
      datum_ende=dende,
      anzahl_quantifier=aq,
      anzahl=anz
    )
  ret = x.beobachtung.update_geometry(str(b.beobachtung_pk), lon, lat)

  logger.info("Processing line (%d/%d) - %s %s (%s)",index, len(df.index),row['Gattung'], row['Art'], row['Trivialname'])

########################################################################################################################
quit()
