# -*- coding: utf-8 -*-
# try something like

@auth.requires(request.client=='127.0.0.1' or auth.is_logged_in(), requires_login=False)
def index():
    return dict(odb=request.args(0))

@auth.requires(request.client=='127.0.0.1' or auth.is_logged_in(), requires_login=False)
def table():
    gcp = "gcp-"
    grid = SQLFORM.grid(odbs[request.args(0)][request.args(1)],
        args = request.args[:2],
        **dict(dict(
            create = False, deletable = False, editable = False,
            csv = False,
            searchable = False,
            paginate = 20,
            user_signature = False
            ),
            #**{k[len(gcp):]: v for k,v in myconf.take(tablename).iteritems() if k.startswith(gcp)}
        )
    )
    return dict(grid=grid)
