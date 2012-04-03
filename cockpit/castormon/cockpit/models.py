from django.db import models


class MetricData(models.Model):
    """
    """
    name = models.CharField(max_length=200, db_index=True)
    timestamp = models.IntegerField(db_index=True)
    data = models.TextField(default="")
    
    def __repr__ (self):
        """
        """
        return '<MetricData: %s %s>' % (self.name, self.timestamp)

class Metric(models.Model):
    """
    metric object, representing a running metric
    """
    name = models.CharField(max_length=200, default="", primary_key=True, db_index=True)
    window = models.IntegerField(default=0)
    conditions = models.TextField(default="")
    groupbykeys = models.TextField(default="")
    data = models.TextField(default="")
    handle_unordered = models.TextField(default="")
    nbins = models.IntegerField(default=0)
    running = models.BooleanField(default=False)
    config_file = models.TextField(default="")
    
    def __repr__ (self):
        """
        """
        return '<Metric: %s>' % (self.name)


