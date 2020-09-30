"""billing-counts

Usage:
  billing-counts.py [--action=<act>] [--amend] --days <date_str>...
  billing-counts.py [--action=<act>] [--amend] --from <date_from> --to <date_to>

Options:
  -h --help             Show this screen.
  --days                List of days to handle, separate by space
  --from                Starting date (use with date_to)
  --to                  Starting date (use with date_from)
  --action=<act>        Restrict to calling this action
  --amend               Amends dataset to current [default: True]

"""
import time
from docopt import docopt
from datetime import timedelta, date, datetime
from functions import (
    createBillingView,
    createNetezzaViews,
    createMperMapperView,
    getBillingPerMperPerDay,
    getBillingGroupsForDate,
    getDates,
    getDateRange,
    getNamedPath,
    initBillingDf,
    initBillingGroupsDf,
    logger,
    readCsv,
    writeCsv
)

def main():
    arguments = docopt(__doc__, version='billing-monthly v0.1')
    if arguments['--days']:
        dates = getDates(arguments['<date_str>'])
    elif arguments['--from'] and arguments['--to'] is True:
        d0 = datetime.strptime(arguments['<date_from>'], "%Y-%m-%d").date()
        d1 = datetime.strptime(arguments['<date_to>'], "%Y-%m-%d").date()
        dates = getDateRange(d0, d1)
    else:
        return logger.info('Bad args.')

    if arguments['--action'] == 'buildBillings':
        buildBillings(dates)
    elif arguments['--action'] == 'buildGroups':
        buildGroups(dates)
    elif arguments['--action'] == 'buildSummary':
        buildSummary(arguments['--amend'])
    elif arguments['--action']:
        return logger.info('Bad action.')
    else:
        buildBillings(dates)
        buildGroups(dates)
        buildSummary(arguments['--amend'])

def buildBillings(dates):
    logger.info('build billing-per-day')
    createNetezzaViews(date(2018,4,1))
    createMperMapperView(date(2018,7,2))
    df = initBillingDf()
    for dt in dates:
        rows = getBillingPerMperPerDay(dt)
        if rows is not None:
            df = df.union(rows)
        else:
            logger.info('df for billing-per-day was None')
    output_path = getNamedPath('billing-per-day')
    writeCsv(df, output_path, repartition=1, partitionBy="date")

def buildGroups(dates):
    logger.info('build billing-groups-daily')
    createBillingView()
    df = initBillingGroupsDf()
    for dt in dates:
        rows = getBillingGroupsForDate(dt)
        if rows is not None:
            df = df.union(rows)
        else:
            logger.info('df for billing-groups-daily was None')
    output_path = getNamedPath('billing-groups-daily')
    writeCsv(df, output_path, repartition=1, partitionBy="date")

def buildSummary(amendToCurrent):
    logger.info('build billing-groups')
    input_path = getNamedPath('billing-groups-daily')
    df = readCsv(input_path).select(["date", "billing", "customers", "total"]).orderBy('date')
    output_path = getNamedPath('billing-groups')

    if amendToCurrent is True:
        writeCsv(df, output_path, repartition=1)
    else:
        writeCsv(df, output_path, repartition=1, mode="overwrite")

main()
