#helper method
def get_current_date():
    """
    get date of german timezone

    :param time: datetime object
    :return: csv-filename, e.g. "2023-05-csv"
    """ 
    # Get the current time in UTC
    current_time = datetime.now(pytz.utc)
    # Convert the current time to the timezone of Berlin
    berlin_timezone = pytz.timezone('Europe/Berlin')
    time = current_time.astimezone(berlin_timezone)
    #return time
    return time