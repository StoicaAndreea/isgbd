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

        mytree = ET.parse('Catalog.xml')
        myroot= mytree.getroot()
        print(myroot.tag)
        # databases=myroot.find("Databases")
        for db in myroot.iter("Database"):
            if db.attrib['dataBaseName'] == dataBaseName:
                tables=db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == data["tableName"]:
                        return "tabelul exista deja"
                #mytree = ET.parse('Catalog.xml')
                #myroot = db.getparent()


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

  def dropTable(self,databaseName, name):

      with open('Catalog.xml', 'r') as f:

          mytree = ET.parse('Catalog.xml')
          myroot = mytree.getroot()
          for db in myroot.iter("Database"):
              if db.attrib['dataBaseName'] == databaseName:
                  tables = db.find("Tables")
                  for tb in tables.iter("Table"):
                      if tb.attrib['tableName'] == name:
                        # myroot = db.getparent()
                        tables.remove(tb)
                        print(myroot)
                        # ET.dump(parent)

                        mytree = ET.ElementTree(myroot)
                        mytree.write('Catalog.xml')
                        return "table dropped succesfully"
      return "table couldnt be found"

  def createIndex(self):
      pass
