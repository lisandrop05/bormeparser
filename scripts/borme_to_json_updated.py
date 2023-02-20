#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# borme_to_json.py - Convert BORME A PDF files to JSON
# Copyright (C) 2015-2017 Pablo Castellano <pablo@anche.no>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import shutil
import bormeparser
import traceback
import bormeparser.backends.pypdf2.parser

from bormeparser.backends.defaults import OPTIONS
OPTIONS['SANITIZE_COMPANY_NAME'] = True

import os


filelist = []
infile = open('/home/lisandro/BormePdfLinks.txt','r')
for line in infile:
	filelist.append(line.split("/")[-1].strip())
infile.close()


for filename in filelist:
	if filename == "":
		print('Empty data: '+filename)
	else:
		if '-99.pdf' not in filename:
	#		print("\n File with Enterprise list. Ignoring...")
	#		print('Not  creating JSON for {}'.format(filename))
	#	else:

			if os.path.exists('/home/lisandro/error_pdfs/'+filename) == False and os.path.exists('/home/lisandro/pdfs/'+filename) == True and os.path.exists('/home/lisandro/json_files/'+filename.split(".")[0]+".json") == False:			
				print('\nParsing {}'.format(filename))
				try:
					borme = bormeparser.parse('/home/lisandro/pdfs/'+filename, bormeparser.SECCION.A)
					path = borme.to_json('/home/lisandro/json_files/')

					if path:
						print('Created {}'.format(os.path.abspath(path)))
					else:
						print('Error creating JSON for {}'.format(filename))
				except Exception as error:
					print(traceback.format_exc())
					print("Error: on PDF")
					shutil.move('/home/lisandro/pdfs/'+filename,'/home/lisandro/error_pdfs/'+filename)			
				
			else:
	#			if os.path.exists('/home/lisandro/json_files/'+filename.split(".")[0]+".json") == True:
	#				print("JSON File already exists")
	#				print('JSON file {}'.format(filename.split(".")[0]+".json")+"\n")
				if os.path.exists('/home/lisandro/pdfs/'+filename) == False:
					print("PDF file does not exists")
					print('/home/lisandro/pdfs/'+filename)
				if os.path.exists('/home/lisandro/error_pdfs/'+filename) == True:
					print("PDF file AlreadyFailed. Moving")
					shutil.move('/home/lisandro/pdfs/'+filename,'/home/lisandro/error_pdfs/'+filename +"1")
