# encoding: utf-8
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models

class Migration(SchemaMigration):

    def forwards(self, orm):
        
        # Adding model 'MetricData'
        db.create_table('cockpit_metricdata', (
            ('id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.CharField')(max_length=200, db_index=True)),
            ('timestamp', self.gf('django.db.models.fields.IntegerField')(db_index=True)),
            ('data', self.gf('django.db.models.fields.TextField')(default='')),
        ))
        db.send_create_signal('cockpit', ['MetricData'])

        # Adding model 'Metric'
        db.create_table('cockpit_metric', (
            ('name', self.gf('django.db.models.fields.CharField')(default='', max_length=200, primary_key=True, db_index=True)),
            ('window', self.gf('django.db.models.fields.IntegerField')(default=0)),
            ('conditions', self.gf('django.db.models.fields.TextField')(default='')),
            ('groupbykeys', self.gf('django.db.models.fields.TextField')(default='')),
            ('data', self.gf('django.db.models.fields.TextField')(default='')),
            ('handle_unordered', self.gf('django.db.models.fields.TextField')(default='')),
            ('nbins', self.gf('django.db.models.fields.IntegerField')(default=0)),
            ('running', self.gf('django.db.models.fields.BooleanField')(default=False)),
        ))
        db.send_create_signal('cockpit', ['Metric'])


    def backwards(self, orm):
        
        # Deleting model 'MetricData'
        db.delete_table('cockpit_metricdata')

        # Deleting model 'Metric'
        db.delete_table('cockpit_metric')


    models = {
        'cockpit.metric': {
            'Meta': {'object_name': 'Metric'},
            'conditions': ('django.db.models.fields.TextField', [], {'default': "''"}),
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
