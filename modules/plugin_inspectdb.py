#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *

def loopOconns(myconf, confkey):
    """ Loops over configured db connections """
    for k in filter(lambda c: c.startswith(confkey), myconf):
        yield k[len(confkey):], \
            {key: myconf.get(".".join((k, key,))) for key in myconf.take(k)}

class InspectDB(object):

    sql = {
        "postgres": {
            "_get_all_tabs": "SELECT tablename FROM pg_tables WHERE schemaname = 'public';",
            "_get_all_views": "select viewname from pg_catalog.pg_views where schemaname NOT IN ('pg_catalog', 'information_schema');",
            "_get_all_fields": """SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type
                FROM pg_attribute a
                JOIN pg_class b ON (a.attrelid = b.relfilenode)
                WHERE b.relname = '%(tablename)s' and a.attstattarget = -1;"""
        }
    }

    def __init__(self, odb):
        self.odb = odb
        self.engine = str(odb._adapter).split('.')[2]

    def get_all_tabs(self):
        return self.odb.executesql(self.sql[self.engine]["_get_all_tabs"])

    def get_all_views(self):
        return self.odb.executesql(self.sql[self.engine]["_get_all_views"])

    def _loopOtabs(self):
        """ Iterates over tables and views """
        for r in self.get_all_tabs()+self.get_all_views():
            yield r[0]

    def loopOtabs(self):
        return list(self._loopOtabs())

    def _loopOfields(self, tablename):
        try:
            res = self.odb.executesql(self.sql[self.engine]["_get_all_fields"] % vars())
        except:
            self.odb.rollback()
        else:
            for r in res:
                yield r

    def loopOfields(self, tablename):
        return list(self._loopOfields(tablename))

    def getAllFieldsConf(self, tablename, **precfg):

        def _get_conf(column_name, data_type):
            conf = {"rname": column_name \
                if not column_name in (column_name.upper(), column_name.capitalize()) \
                else '"%s"' % column_name
            }
            if column_name == "id":
                conf.update({"type": "id"})
            elif data_type.startswith("character varying"):
                conf.update({"type": "string", "length": int(data_type[18:-1])})
            elif data_type.startswith("character"):
                conf.update({"type": "string", "length": int(data_type[10:-1])})
            elif data_type=="text":
                conf.update({"type": "text"})
            elif data_type.startswith("geometry"):
                conf.update({"type": "geometry()"})
            elif data_type == "integer":
                conf.update({"type": "integer"})
            elif data_type == "double precision":
                conf.update({"type": "double"})
            elif data_type=="timestamp without time zone":
                conf.update({"type": "datetime"})
            elif data_type in ("date", "json",):
                conf.update({"type": data_type})
            else:
                raise NotImplementedError()

            if precfg.get(column_name)=="id":
                conf["fieldname"] = "id"
                conf["type"] = "id"

#             if "inspectdb" in myconf and column_name in myconf.take("inspectdb"):
#                 conf["fieldname"] = myconf.take("inspectdb."+column_name)
#                 if myconf.take("inspectdb."+column_name) == "id":
#                     conf["type"] = "id"

            return conf

        for r in self.loopOfields(tablename):
            try:
                yield r[0], dict(_get_conf(*r), fieldname=r[0])
            except:
                pass

    def getFields(self, tablename, **precfg):
# #             if tablename in myconf and "fields" in myconf.take(tablename):
# #                 fieldnames = myconf.take(tablename+".fields", cast=lambda v: v.split(","))
# #                 rawconfs = {k: v for k,v in cls._pg_allfields(odb, tablename)}
# #                 #import pdb; pdb.set_trace()
# #                 for fn in fieldnames:
# #                     fconf = rawconfs[fn]
# #                     if tablename+":"+fn in myconf:
# #                         fconf.update(myconf.take(tablename+":"+fn))
# #                     yield Field(**fconf)
#             else:
            for fn, fconf in self.getAllFieldsConf(tablename, **precfg):
                k = tablename+":"+fn
#                 if k in myconf:
#                     fconf.update(myconf.take(k))
                yield Field(**fconf)

    def getAllFields(self, *a, **kw):
        return list(self.getFields(*a, **kw))
        