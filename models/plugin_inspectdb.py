# -*- coding: utf-8 -*-

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

response.menu += [
    (STRONG(SPAN(_class="glyphicon glyphicon-sunglasses", **{"_aria-hidden": "true"}), " ", T("Inspect dbs"), _style="color: yellow;"), False, "#", [
        (conn, False, URL("plugin_inspectdb", "index", args=(conn,)),) \
    for conn in odbs],),
]