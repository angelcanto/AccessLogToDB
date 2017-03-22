import re
import datetime
from datetime import date
from datetime import timedelta
import geoip2.database
from ua_parser import user_agent_parser
import xlsxwriter
import os
import pysftp
import gzip
import os
import pypyodbc
def GetFileName():
	path = "./data/access.log."
	months_esp =['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
	yesterday = date.today() - timedelta(days=1)
	return path+str(yesterday.day)+months_esp[yesterday.month-1]+str(yesterday.year)
def GetDate(str_date):
	months_eng =['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
	months_esp =['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
	split1 = str_date.split(':')
	split2 = split1[0].split('/')
	split3 = split1[-1].split(' ')
	try:
		int_month = months_eng.index(split2[1])+1
	except ValueError:
		int_month = months_esp.index(split2[1])+1
	return datetime.datetime(int(split2[2]), int_month, int(split2[0]), int(split1[1]), int(split1[2]),int(split3[0]))
connection = pypyodbc.connect('Driver={SQL Server};Server=XXXX;Database=XXXXX;uid=XXXXXX;pwd=XXXXXX')
cursor = connection.cursor()
SQLCommand = ("INSERT INTO XXXXX (Fecha, Anyo, Mes,Dia,Fabricante_Dispositivo,Familia_Dispositivo,Modelo_Dispositivo,Familia_Sistema_Operativo,Version_Sistema_Operativo,Familia_Navegador,Version_Navegador,IP,Pais,Region,Ciudad,Latitud,Longitud,Peticion,Origen,Servidor) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
cursor.execute("TRUNCATE TABLE XXXX") #Truncate if the GeoLite DB is updated
ftpfiles = []
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
#SFTP Connection details
with pysftp.Connection('XXXXX', username='XXXXX', password='XXXXXX', cnopts=cnopts) as sftp:
	with sftp.cd('logs'):         
		for attr in sftp.listdir_attr():
			if attr.filename.endswith('.gz') and attr.filename.startswith('access.log'):
				ftpfiles.append((attr.filename, int(attr.st_mtime)))
		ftpfiles.sort(key=lambda x: x[1])
		sftp.get(ftpfiles[-1][0], preserve_mtime=True)
f=gzip.open(ftpfiles[-1][0],'rb')
file_content=f.read()
with open(GetFileName(),"w+") as ftemp:
    ftemp.write(file_content)
f.close()
os.remove(ftpfiles[-1][0])
workbook = xlsxwriter.Workbook('webdata.xlsx')
dateformat = workbook.add_format({'num_format': 'dd/mm/yy'})
worksheet = workbook.add_worksheet()
worksheet.write(0, 0, 'Fecha')
worksheet.write(0, 1, 'Anyo')
worksheet.write(0, 2, 'Mes')
worksheet.write(0, 3, 'Dia')
worksheet.write(0, 4, 'Fabricante Dispositivo')
worksheet.write(0, 5, 'Familia Dispositivo')
worksheet.write(0, 6, 'Modelo Dispositivo')
worksheet.write(0, 7, 'Familia Sistema Operativo')
worksheet.write(0, 8, 'Version Sistema Operativo')
worksheet.write(0, 9, 'Familia Navegador')
worksheet.write(0, 10, 'Version Navegador')
worksheet.write(0, 11, 'IP')
worksheet.write(0, 12, 'Pais')
worksheet.write(0, 13, 'Region')
worksheet.write(0, 14, 'Ciudad')
worksheet.write(0, 15, 'Latitud')
worksheet.write(0, 16, 'Longitud')
worksheet.write(0, 17, 'Peticion')
worksheet.write(0, 18, 'Origen')
worksheet.write(0, 19, 'Servidor')
row = 1
regexhtml = re.compile('(.+) - - \[(.+)\] \"GET \/(.+\.html) .+\" \d+ \d+ (.+) \"(.+)\" \"(.+)\" \".+\"')
regexpdf  = re.compile('(.+) - - \[(.+)\] \"GET \/(.+\.pdf) .+\" \d+ \d+ (.+) \"(.+)\" \"(.+)\" \".+\"')
reader = geoip2.database.Reader('./mmdb/GeoLite2-City.mmdb') #Requires GeoLite2-City.mmdb
for file in os.listdir("./data"):
	with open("./data/"+file) as f:
		for line in f:
			details = regexhtml.findall(line)
			details += regexpdf.findall(line)
			if len(details) == 0:
				continue
			VisitDate = GetDate(details[0][1])
			worksheet.write(row, 0, VisitDate.date(),dateformat)
			worksheet.write(row, 1, VisitDate.year)
			worksheet.write(row, 2, VisitDate.month)
			worksheet.write(row, 3, VisitDate.day)
		
		
			parsed_string = user_agent_parser.Parse(details[0][5])
			worksheet.write(row, 4, parsed_string['device']['brand'])
			worksheet.write(row, 5, parsed_string['device']['family'])
			worksheet.write(row, 6, parsed_string['device']['model'])
			worksheet.write(row, 7, parsed_string['os']['family'])
			worksheet.write(row, 8, parsed_string['os']['major'])
			worksheet.write(row, 9, parsed_string['user_agent']['family'])
			worksheet.write(row, 10, parsed_string['user_agent']['major'])
			
			
			worksheet.write(row, 11, details[0][0])
			try:
				response = reader.city(details[0][0])
			except ValueError:
				worksheet.write(row, 12, 'N/A')
				worksheet.write(row, 13, 'N/A')
				worksheet.write(row, 14, 'N/A')
				worksheet.write(row, 15, 'N/A')
				worksheet.write(row, 16, 'N/A')
			else:
				worksheet.write(row, 12, response.country.name)
				worksheet.write(row, 13, response.subdivisions.most_specific.name)
				worksheet.write(row, 14, response.city.name)
				worksheet.write(row, 15, response.location.latitude)
				worksheet.write(row, 16, response.location.longitude)
			
			worksheet.write(row, 17, details[0][2])
			servername = details[0][3]
			if servername.startswith('www.'):
				servername = servername[4:]
			worksheet.write(row, 19, servername)
			worksheet.write(row, 18, details[0][4])
			SQLValues = [VisitDate.date(),VisitDate.year,VisitDate.month,VisitDate.day,parsed_string['device']['brand'],parsed_string['device']['family'],parsed_string['device']['model'],parsed_string['os']['family'],parsed_string['os']['major'],parsed_string['user_agent']['family'],parsed_string['user_agent']['major'],details[0][0],response.country.name,response.subdivisions.most_specific.name,response.city.name,response.location.latitude,response.location.longitude,details[0][2],details[0][4],servername]
			cursor.execute(SQLCommand,SQLValues)
			row += 1
workbook.close()
connection.commit()
connection.close()