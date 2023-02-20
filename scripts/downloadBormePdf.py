from datetime import timedelta
from borme_classes import *

BORME_ROOT = "./"
FIRST_BORME = {2009: datetime.date(2009, 1, 2),
               2010: datetime.date(2010, 1, 4),
               2011: datetime.date(2011, 1, 3),
               2012: datetime.date(2012, 1, 2),
               2013: datetime.date(2013, 1, 2),
               2014: datetime.date(2014, 1, 2),
               2015: datetime.date(2015, 1, 2)}


def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download BORME PDF files.')
    parser.add_argument('-f', '--fromdate', default='today',
                        help='ISO formatted date (ex. 2015-01-01) or "init". Default: today')
    parser.add_argument('-t', '--to', default='today', help='ISO formatted date (ex. 2016-01-01). Default: today')
    parser.add_argument('-d', '--directory', default=BORME_ROOT, help='Directory to download files (default is {})'.format(BORME_ROOT))
    parser.add_argument('-o', '--old_directory', default=None,
                        help='Directory of old downloaded files (default is {})'.format(str(None)))

    args = parser.parse_args()

    if args.fromdate == 'init':
        date_from = FIRST_BORME[2009]
    elif args.fromdate == 'today':
        date_from = datetime.date.today()
    else:
        date_from = datetime.datetime.strptime(args.fromdate, '%Y-%m-%d').date()

    if args.to == 'today':
        date_to = datetime.date.today()
    else:
        date_to = datetime.datetime.strptime(args.to, '%Y-%m-%d').date()

    weekdays = [6, 7]

    dates = []
    for dt in daterange(date_from, date_to):
        print(dt)
        print(dt.isoweekday())
        if dt.isoweekday() not in weekdays:
            dates.append(dt)
    download_all_borme_content(dates, pMainDataPath=args.directory, pOldDataPath=args.old_directory)
    # bormeDownloader = BormeDayDownloader(date, pOldDataPath="./old/")
    # bormeDownloader.prepare_content()
    # bormeDownloader.download_day_content()
    # https://boe.es/diario_borme/xml.php?id=BORME-S-20150910

