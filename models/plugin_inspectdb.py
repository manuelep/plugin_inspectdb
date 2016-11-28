# -*- coding: utf-8 -*-

import json

def _plugin_inspectdb():

    from plugin_inspectdb import InspectDB, loopOconns
    from gluon.tools import PluginManager
    plugins = PluginManager('inspectdb', confkey="inspectdb:")

    odbs = {name: DAL(nfo["uri"], pool_size=nfo["pool_size"], migrate=nfo["migrate"], check_reserved=['all']) \
        for name, nfo in loopOconns(myconf, plugins.inspectdb.confkey)}

    for k, odb in odbs.iteritems():
        odbInspector = InspectDB(odb)
        # myconf.take(k+".tables", cast=lambda v: v.split(','))
        odb_tables =  myconf.get(plugins.inspectdb.confkey+k, {}).get("tables") and \
            myconf.get(plugins.inspectdb.confkey+k, {}).get("tables") .split(',') or \
            odbInspector._loopOtabs()

        for tablename in odb_tables:
            if tablename in myconf:
                tabconf = {k[1:]: v for k,v in myconf.take(tablename).iteritems() if k.startswith("_")}
            else:
                tabconf = {}

            precfg = {} if not "inspectdb" in myconf else myconf["inspectdb"]
            fields = odbInspector.getAllFields(tablename, **precfg)

            if any([(f.type=="id") for f in fields]):
                odbs[k].define_table(tablename, *fields, **tabconf)

    return odbs

odbs = _plugin_inspectdb()

class DBService(object):
    """ """

    @staticmethod
    def _cast(f, v):
        """
        f @Field  : object;
        v @string : value to be inserted
        """
        if f.type in (None, 'string', 'text'):
            return v
        elif f.type == 'integer':
            return int(v)
        elif f.type == 'boolean':
            return bool(v)
        elif f.type == 'double':
            return float(v)
        elif f.type == 'date':
            return  dateutil.parser.parse(v).date()
        elif f.type == 'datetime':
            return dateutil.parser.parse(v)
        elif f.type == 'json':
            return json.loads(v) if isinstance(v, basestring) else v
        else:
            raise NotImplementedError()

    @classmethod
    def insert(cls, dbname, tablename, **kw):
        """ """
        @auth.requires_login()
        def _main():
            tab = odbs[dbname][tablename]
            return tab.insert(**{k: cls._cast(tab[k], v) for k,v in kw.iteritems()})
        return _main()

    @classmethod
    def bulk_insert(cls, dbname, tablename, _data):
        """
        data @list : list of dictionaries
        """
        data = json.loads(_data)
        @auth.requires_login()
        def _main():
            return odbs[dbname][tablename].bulk_insert(map(lambda kw: {k: cls._cast(tab[k], v) for k,v in kw.iteritems()}, data))
        return _main()

@service.json
def db_insert(dbname, tablename, **kw):
    """ """
    return dict(id = DBService.insert(dbname, tablename, **kw))

@service.json
def db_bulk_insert(dbname, tablename, data):
    """ """
    return dict(ids = DBService.bulk_insert(dbname, tablename, data))

response.menu += [
    (STRONG(SPAN(_class="glyphicon glyphicon-sunglasses", **{"_aria-hidden": "true"}), " ", T("Inspect dbs"), _style="color: yellow;"), False, "#", [
        (conn, False, URL("plugin_inspectdb", "index", args=(conn,)),) \
    for conn in odbs],),
]
