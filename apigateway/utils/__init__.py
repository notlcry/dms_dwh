def singleton(cls):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return _singleton