import xml.etree.ElementTree as ET


from io import BytesIO
from xml.etree import ElementTree as ET
import os


class CatalogController:
  def __init__(self):
      # TODO habarnam cum sa scriu  la inceput de fisier et.write(f, encoding='utf-8', xml_declaration=True)

      if os.path.exists('Catalog.xml'):
        pass
      else:
        #daca nu exista fisierul
        databases = ET.Element('Databases')
        tree = ET.ElementTree(databases)
        tree.write('Catalog.xml')

  def createDatabase(self, name):

      with open('Catalog.xml', 'r') as f:
        doc = ET.parse(f)
        for elem in doc.iter("Database"):
          if elem.attrib['dataBaseName'] == name:
              return "Baza de date exista deja"

        mytree = ET.parse('Catalog.xml')
        myroot = mytree.getroot()
        database = ET.SubElement(myroot, 'Database',dataBaseName=name)
        tables = ET.SubElement(database, 'Tables')
        ET.dump(tables)
        mytree.write('Catalog.xml')
      return "Baza de date a fost adaugata"

  def dropDatabase(self, name):
      # TODO check if database exists

      with open('Catalog.xml', 'r') as f:
        doc = ET.parse(f)
        for db in doc.iter("Database"):
          if db.attrib['dataBaseName'] == name:
              myroot = db.getparent()
              myroot.remove(db)
              print(myroot)
              # ET.dump(parent)
              mytree = ET.ElementTree(myroot)
              mytree.write('Catalog.xml')
              return "database dropped succesfully"
      return "database couldnt be found"


  def createTable(self,dataBaseName, data):

      with open('Catalog.xml', 'r') as f:
        doc = ET.parse(f)
        for db in doc.iter("Database"):
            if db.attrib['dataBaseName'] == dataBaseName:
                tables=db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == data["tableName"]:
                        return "tabelul exista deja"
                mytree = ET.parse('Catalog.xml')
                myroot = tables


                table = ET.SubElement(tables, 'Table', tableName=data["tableName"],fileName=data["tableName"]+".bin" ,rowLength="0")
                structure=ET.SubElement(table,'Structure')
                #todo for in care parcurc si adaug atributele
                #-----
                primaryKeys = ET.SubElement(table, 'primaryKey')
                uniqueKeys  = ET.SubElement(table,"uniqueKeys")
                indexFiles  = ET.SubElement(table,"IndexFiles")


                mytree = ET.ElementTree(myroot)
                mytree.write('Catalog.xml')
                return "success creare tabel"
      return "Nu exista baza de date"

  # mytree = ET.parse('Catalog.xml')
  # myroot = mytree.getroot()
  # for db in myroot:
  #     if db.attrib['dataBaseName'] == dataBaseName:
  #         for tb in db.iter("tableName"):
  #             if tb.attrib['tableName'] == data["tableName"]:
  #                 return "tabelul exista deja"

  def dropTable(self,databaseName, name):
      # TODO check if table exists
      mytree = ET.parse('Catalog.xml')
      myroot = mytree.getroot()
      found = False
      for db in myroot:
          if db.attrib['dataBaseName'] == databaseName:
              for tb in db.iter("tableName"):
                  if tb.attrib['tableName'] == name:
                    tb.attrib.pop('tableName')
                    found = True
                  else:
                    parent = tb.getparent()
                    parent.remove(tb)

      if found == True:
        return "Tabelul a fost sters"
      else:
        return "nu a fost gasit tabelul"


  def createIndex(self):
      pass
