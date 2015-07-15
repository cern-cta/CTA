# encoding: utf-8
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models

class Migration(SchemaMigration):

    def forwards(self, orm):
        
        # Adding field 'Metric.config_file'
        db.add_column('cockpit_metric', 'config_file', self.gf('django.db.models.fields.TextField')(default=''), keep_default=False)


    def backwards(self, orm):
        
        # Deleting field 'Metric.config_file'
        db.delete_column('cockpit_metric', 'config_file')


    models = {
        'cockpit.metric': {
            'Meta': {'object_name': 'Metric'},
            'conditions': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'config_file': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'data': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'groupbykeys': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'handle_unordered': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'name': ('django.db.models.fields.CharField', [], {'default': "''", 'max_length': '200', 'primary_key': 'True', 'db_index': 'True'}),
            'nbins': ('django.db.models.fields.IntegerField', [], {'default': '0'}),
            'running': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'window': ('django.db.models.fields.IntegerField', [], {'default': '0'})
        },
        'cockpit.metricdata': {
            'Meta': {'object_name': 'MetricData'},
            'data': ('django.db.models.fields.TextField', [], {'default': "''"}),
            'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '200', 'db_index': 'True'}),
            'timestamp': ('django.db.models.fields.IntegerField', [], {'db_index': 'True'})
        }
    }

    complete_apps = ['cockpit']
