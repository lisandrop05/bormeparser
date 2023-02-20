import os
import shutil
import datetime
import json
import time
from lxml import etree
import requests
from threading import Thread
from queue import Queue
import argparse

THREADS = 10
DAYTHREADS = 5


def download_all_borme_content(dateList, pMainDataPath="./", pOldDataPath=None):
    q = Queue()
    files = []
    for i in range(DAYTHREADS):
        t = ThreadDownloadBormeDayUrls(i, q, files)
        t.setDaemon(True)
        t.start()

    for date in dateList:
        q.put((date, pMainDataPath, pOldDataPath))
    q.join()
    return files


def download_borme_url(url, filename=None, try_again=0):
    if os.path.exists(filename):
        return True
    try:
        req = requests.get(url, stream=True, timeout=20)
    except Exception as e:
        print('%s failed to download (%d time)!' % (url, try_again + 1))
        if try_again < 3:
            return download_borme_url(url, filename=filename, try_again=try_again + 1)
        else:
            raise e
    with open(filename, "wb") as fp:
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                fp.write(chunk)
    if 'content-length' in req.headers:
        content_length = req.headers['content-length']
        print("%.2f KB" % (int(content_length) / 1024.0))
    return True


class ThreadDownloadUrl(Thread):
    """Threaded Url Grab"""

    def __init__(self, thread_id, queue, files):
        super(ThreadDownloadUrl, self).__init__()
        self.thread_id = thread_id
        self.queue = queue
        self.files = files

    def run(self):
        while True:
            url, full_path = self.queue.get()
            # time.sleep(0.1)
            downloaded = download_borme_url(url, full_path)
            if downloaded:
                self.files.append(full_path)
                print('Downloaded %s' % os.path.basename(full_path))
            self.queue.task_done()


class ThreadDownloadBormeDayUrls(Thread):
    """Threaded Url Grab"""

    def __init__(self, thread_id, queue, files):
        super(ThreadDownloadBormeDayUrls, self).__init__()
        self.thread_id = thread_id
        self.queue = queue
        self.files = files

    def run(self):
        while True:
            date, mainDataPath, oldDataPath = self.queue.get()
            # time.sleep(0.1)
            create_date_instance(date, mainDataPath, oldDataPath)
            self.queue.task_done()


def create_date_instance(date, mainDataPath, oldDataPath):
    bormeDownloader = BormeDayDownloader(date, pMainDataPath=mainDataPath, pOldDataPath=oldDataPath)
    count = 0
    response = bormeDownloader.prepare_content()
    while not response and count < 60:
        time.sleep(1)
        count = count+1
        response = bormeDownloader.prepare_content()

    if response:
        bormeDownloader.download_day_content()


def parse_content(content):
    # Python 3
    found = False
    if isinstance(content, bytes):
        content = content.decode('unicode_escape')
    if '<error>' not in content:
        # print('AVAILABLE! (%d bytes)' % len(content))
        found = True
    return found


def parse_date(date):
    return datetime.datetime.strptime(date, '%d/%m/%Y').date()


class UrlDownloader:
    def download_borme_url(self, url, filename=None, try_again=0):
        if os.path.exists(filename):
            return True
        try:
            req = requests.get(url, stream=True, timeout=20)
        except Exception as e:
            print('%s failed to download (%d time)!' % (url, try_again + 1))
            if try_again < 3:
                return self.download_borme_url(self, url, filename=filename, try_again=try_again + 1)
            else:
                raise e
        with open(filename, "wb") as fp:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    fp.write(chunk)
        if 'content-length' in req.headers:
            content_length = req.headers['content-length']
            print("%.2f KB" % (int(content_length) / 1024.0))
        return True


class UrlListManager:
    def __init__(self, pFileListPath, pOutputPath, pVerifyPath=None):
        self.downloader = UrlDownloader()
        self.fileList = []
        self.fileListPath = ""
        self.outputPath = ""
        self.verifyPath = ""
        self.fileHandler = None
        self.fileListPath = pFileListPath
        self.outputPath = pOutputPath
        self.verifyPath = pVerifyPath
        self.fileHandler = open(self.fileListPath, 'r')
        for line in self.fileHandler:
            self.fileList.append("https://www.boe.es" + line.strip())
        self.fileHandler.close()

    def download_all_urls(self):
        q = Queue()
        files = []
        for i in range(THREADS):
            t = ThreadDownloadUrl(i, q, files)
            t.setDaemon(True)
            t.start()

        for url in self.fileList:
            filename = url.split('/')[-1]
            if ".pdf" not in filename:
                filename = filename.split("=")[-1]
                filename = filename + ".xml"
            full_path = None
            if "BORME-A" in filename:
                full_path = os.path.join(self.outputPath + "/A/", filename)
            if "BORME-B" in filename:
                full_path = os.path.join(self.outputPath + "/B/", filename)
            if "BORME-C" in filename:
                full_path = os.path.join(self.outputPath + "/C/", filename)
            if self.verifyPath is not None:
                if os.path.exists(self.verifyPath + filename):
                    shutil.move(self.verifyPath + filename, full_path)
                else:
                    q.put((url, full_path))
            else:
                q.put((url, full_path))
        q.join()
        return files


class BormeDayDownloader:
    def __init__(self, date, pTIMEOUT=10, pMainDataPath="./", pOldDataPath=None):
        self.year = ""
        self.month = ""
        self.day = ""
        self.nbo = None
        self.mainDataPath = "./"
        self.todayDataPath = None
        self.bormeDate = None
        self.linksFilePath = None
        self.linksFileHandler = None
        self.prev_borme = None
        self.next_borme = None
        self.dailyXmlUrl = ""
        self.urlManager = None
        self.is_final = False
        self.oldDataPath = None
        self.xml = ""
        self.TIMEOUT = 10
        strdate = f"{date:%Y/%m/%d}"
        # strdate = date.format("Y/M/d")
        self.day = strdate.split("/")[-1]
        self.month = strdate.split("/")[-2]
        self.year = strdate.split("/")[-3]
        self.TIMEOUT = pTIMEOUT
        self.mainDataPath = pMainDataPath
        self.dailyXmlUrl = "https://boe.es/diario_borme/xml.php?id=BORME-S-" + self.year + self.month + self.day
        self.oldDataPath = pOldDataPath
        self.errorLinks = os.path.join(self.mainDataPath, "./error_links.txt")
        self.errorDataPath = os.path.join(self.mainDataPath, "./errors/")

    def create_directory(self):
        if not os.path.isdir(self.mainDataPath + "/" + str(self.year)):
            os.mkdir(self.mainDataPath + "/" + str(self.year))
        if not os.path.isdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month)):
            os.mkdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month))
        if not os.path.isdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day)):
            os.mkdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day))
        if not os.path.isdir(
                self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/A"):
            os.mkdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/A")
        if not os.path.isdir(
                self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/B"):
            os.mkdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/B")
        if not os.path.isdir(
                self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/C"):
            os.mkdir(self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day) + "/C")
        self.todayDataPath = self.mainDataPath + "/" + str(self.year) + "/" + str(self.month) + "/" + str(self.day)
        # print(self.todayDataPath)

    def download_day_content(self):
        self.urlManager = UrlListManager(pFileListPath=self.linksFilePath, pOutputPath=self.todayDataPath,
                                         pVerifyPath=self.oldDataPath)
        self.urlManager.download_all_urls()

    def prepare_content(self):
        req = requests.get(self.dailyXmlUrl, timeout=self.TIMEOUT)
        found = parse_content(req.text)
        if found:
            content = req.text.encode(req.encoding)
            try:
                self.xml = etree.fromstring(content).getroottree()
            except Exception as err:
                print(err)
                errorLinkFile = open(self.errorLinks, 'a')
                errorLinkFile.write(self.dailyXmlUrl)
                errorLinkFile.close()
                if not os.path.exists(self.errorDataPath):
                    os.mkdir(self.errorDataPath)
                errorLinkFile = open(os.path.join(self.errorDataPath) + self.dailyXmlUrl.split("/")[-1], 'w')
                errorLinkFile.write(req.text)
                errorLinkFile.close()
            if isinstance(self.xml, str):
                return False

            if self.xml.getroot().tag == 'sumario':
                self.bormeDate = parse_date(self.xml.xpath('//sumario/meta/fecha')[0].text)
                self.nbo = int(self.xml.xpath('//sumario/diario')[0].attrib['nbo'])
                self.prev_borme = parse_date(self.xml.xpath('//sumario/meta/fechaAnt')[0].text)
                self.next_borme = self.xml.xpath('//sumario/meta/fechaSig')[0].text

            if self.next_borme:
                self.next_borme = parse_date(self.next_borme)
                self.is_final = True

            if self.is_final:
                self.create_directory()
                json_data_content = []
                if os.path.isdir(self.todayDataPath):
                    self.linksFilePath = self.todayDataPath + "/list_links.txt"
                    linksList = []
                    self.linksFileHandler = open(self.linksFilePath, 'w')
                    for item in self.xml.xpath('//sumario/diario/seccion'):
                        if item.attrib['num']:
                            # print(item.attrib['num'] + ":" + item.attrib['nombre'])
                            segment_data = {}
                            if item.attrib['num'] == 'A':
                                segment_data['emitter'] = {
                                    'nombre': item.xpath('.//emisor')[0].attrib['nombre'],
                                    'etq': item.xpath('.//emisor')[0].attrib['etq']
                                }
                                # print("Emisor: " + item.xpath('.//emisor')[0].attrib['nombre'])
                                # print("Label: " + item.xpath('.//emisor')[0].attrib['etq'] + "\n")
                                data_a = []
                                for element in item.xpath('.//item'):
                                    data_a_item = {
                                        'id': element.attrib['id'],
                                        'province': element.xpath(".//titulo")[0].text,
                                        'size': element.xpath(".//urlPdf")[0].attrib['szBytes'],
                                        'pdfUrl': element.xpath(".//urlPdf")[0].text
                                    }
                                    data_a.append(data_a_item)
                                    # print("ID: " + element.attrib['id'])
                                    # print("Province: " + element.xpath(".//titulo")[0].text)
                                    # print("Size Bytes: " + element.xpath(".//urlPdf")[0].attrib['szBytes'])
                                    # print("Size KBytes: " + element.xpath(".//urlPdf")[0].attrib['szKBytes'])
                                    # print("Url: " + element.xpath(".//urlPdf")[0].text)
                                    if element.xpath(".//urlPdf")[0].text not in linksList:
                                        self.linksFileHandler.write(element.xpath(".//urlPdf")[0].text + "\n")
                                        linksList.append(element.xpath(".//urlPdf")[0].text)
                                segment_data['emitter']['data'] = data_a

                            if item.attrib['num'] == 'B':
                                segment_data['emitter'] = {
                                    'nombre': item.xpath('.//emisor')[0].attrib['nombre'],
                                    'etq': item.xpath('.//emisor')[0].attrib['etq']
                                }
                                # print("Emisor: " + item.xpath('.//emisor')[0].attrib['nombre'])
                                # print("Label: " + item.xpath('.//emisor')[0].attrib['etq'] + "\n")
                                data_b = []
                                for element in item.xpath('.//item'):
                                    data_b_item = {
                                        'id': element.attrib['id'],
                                        'province': element.xpath(".//titulo")[0].text,
                                        'size': element.xpath(".//urlPdf")[0].attrib['szBytes'],
                                        'pdfUrl': element.xpath(".//urlPdf")[0].text
                                    }
                                    data_b.append(data_b_item)
                                    # print("ID: " + element.attrib['id'])
                                    # print("Province: " + element.xpath(".//titulo")[0].text)
                                    # print("Size Bytes: " + element.xpath(".//urlPdf")[0].attrib['szBytes'])
                                    # print("Size KBytes: " + element.xpath(".//urlPdf")[0].attrib['szKBytes'])
                                    # print("Url: " + element.xpath(".//urlPdf")[0].text)
                                    if element.xpath(".//urlPdf")[0].text not in linksList:
                                        self.linksFileHandler.write(element.xpath(".//urlPdf")[0].text + "\n")
                                        linksList.append(element.xpath(".//urlPdf")[0].text)
                                segment_data['emitter']['data'] = data_b

                            if item.attrib['num'] == 'C':
                                segment_data = []
                                for emmiter in item.xpath('.//emisor'):
                                    element_segment_data = {'emitter': {
                                        'nombre': emmiter.attrib['nombre'],
                                        'etq': emmiter.attrib['etq'],
                                        'data': []
                                    }}
                                    # print("Emisor: " + emmiter.attrib['nombre'])
                                    # print("Label: " + emmiter.attrib['etq'] + "\n")
                                    for element in emmiter.xpath('.//item'):
                                        data_c_item = {
                                            'id': element.attrib['id'],
                                            'name': element.xpath(".//titulo")[0].text,
                                            'size': element.xpath(".//urlPdf")[0].attrib['szBytes'],
                                            'pdfUrl': element.xpath(".//urlPdf")[0].text,
                                            'xmlUrl': element.xpath(".//urlXml")[0].text
                                        }
                                        element_segment_data['emitter']['data'].append(data_c_item)
                                        # print("ID: " + element.attrib['id'])
                                        # print("Province: " + element.xpath(".//titulo")[0].text)
                                        # print("Pdf Size Bytes: " + element.xpath(".//urlPdf")[0].attrib['szBytes'])
                                        # print("Pdf Size KBytes: " + element.xpath(".//urlPdf")[0].attrib['szKBytes'])
                                        # print("Pdf Url: " + element.xpath(".//urlPdf")[0].text)
                                        if element.xpath(".//urlPdf")[0].text not in linksList:
                                            self.linksFileHandler.write(element.xpath(".//urlPdf")[0].text + "\n")
                                            linksList.append(element.xpath(".//urlPdf")[0].text)
                                        # print("XML Url: " + element.xpath(".//urlXml")[0].text)
                                        if element.xpath(".//urlXml")[0].text not in linksList:
                                            self.linksFileHandler.write(element.xpath(".//urlXml")[0].text + "\n")
                                            linksList.append(element.xpath(".//urlXml")[0].text)
                                    segment_data.append(element_segment_data)
                                section_data_content = {
                                    'type': item.attrib['num'],
                                    'name': item.attrib['nombre'],
                                    'segment': segment_data
                                }
                                json_data_content.append(section_data_content)
                            else:
                                section_data_content = {
                                    'type': item.attrib['num'],
                                    'name': item.attrib['nombre'],
                                    'segment': segment_data
                                }
                                json_data_content.append(section_data_content)
                        # print(json.dumps(json_data_content))

                self.linksFileHandler.close()
                json_description = self.todayDataPath + "/today_data_resume.json"
                json_description_file = open(json_description, 'w')
                json_description_file.write(json.dumps(json_data_content))
                json_description_file.close()
                return True
            else:
                return False


