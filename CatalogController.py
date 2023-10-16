import xml.etree.ElementTree as ET
import lxml.etree as le

from io import BytesIO
from xml.etree import ElementTree as ET


class CatalogController:
  def __init__(self):
      # TODO habarnam cum sa scriu  la inceput de fisier et.write(f, encoding='utf-8', xml_declaration=True)
      #daca documentul exista deja nu se mai face ce e dedesupt
      self.databases = ET.Element('Databases')
      tree = ET.ElementTree(self.databases)
      tree.write('Catalog.xml')

  def createDatabase(self,name):
      #TODO check if database exists
      database = ET.SubElement(self.databases, 'Database',dataBaseName=name)
      tables = ET.SubElement(database,"Tables")
      tree = ET.ElementTree(self.databases)
      tree.write('Catalog.xml')

  def dropDatabase(self, name):
      # TODO check if database exists
      with open('Catalog.xml', 'r') as f:
          doc = le.parse(f)
          for elem in doc.xpath('//*[attribute::dataBaseName]'):
              if elem.attrib['dataBaseName'] == name:
                  elem.attrib.pop('dataBaseName')
              else:
                  parent = elem.getparent()
                  parent.remove(elem)


  def createTable(self, name):
      # TODO check if table exists
      pass
  def dropTable(self, name):
      # TODO check if table exists
      pass

  def createIndex(self):
      pass
