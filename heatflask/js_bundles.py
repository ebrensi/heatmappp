import flask_assets

# we bundle javascript and css dependencies to reduce client-side overhead
bundles = {
    "dependencies_css": flask_assets.Bundle(
        'css/main.css',
        'css/jquery-ui.css',
        'css/bootstrap.min.css',
        'css/font-awesome.min.css',
        'css/leaflet.css',
        'css/leaflet-sidebar.min.css',
        'css/L.Control.Window.css',
        'css/leaflet-areaselect.css',
        'css/datatables.min.css',
        'css/easy-button.css',
        filters='cssmin',
        output='gen/main.css'
    ),
    "dependencies_js": flask_assets.Bundle(
        # minified dependencies
        flask_assets.Bundle(
            'js/pws.min.js', # persistent websocket https://github.com/porsager/pws
            'js/msgpack.min.js',
            'js/jquery-3.2.1.min.js',
            'js/jquery-ui.min.js',
            'js/jquery.knob.min.js',  # Anthony Terrien
            'js/datatables.min.js',
            'js/leaflet.js',
            'js/leaflet-sidebar.min.js',
            'js/download.min.js',
            'js/gif2.js',  # Johan Nordberg: http://jnordberg.github.io/gif.js/ 
            output="gen/pre-compiled-dependencies.js"
        ),
        # un-minified dependencies
        flask_assets.Bundle(
            'js/moment.min.js',
            'js/Polyline.encoded.js',
            'js/L.Control.Window.js',
            'js/leaflet-providers.js',
            'js/leaflet-image.js',  # Tom MacWright: https://github.com/mapbox/leaflet-image
            'js/leaflet-areaselect.js',
            'js/easy-button.js',
            filters=["babel", "rjsmin"],
            output="gen/build/non-compiled-dependencies.js"
        ),
        output='gen/dependencies.js'
    ),

    "gifjs_webworker_js": flask_assets.Bundle(
        'js/gif.worker.js',
        output="gen/gif.worker.js"
    ),

    "app_specific_js": flask_assets.Bundle(  # Heatflask-specific code
        'js/L.Control.fps.js',
        'js/appUtil.js',
        'js/L.SwipeSelect.js',
        'js/L.BoxHook.js',
        '../heatflask.js',
        '../DotLayer.js',
        filters=["babel", 'rjsmin'],
        output="gen/app-specific.js"
    ),

    "splash_css": flask_assets.Bundle(
        'css/bootstrap.min.css',
        'css/cover.css',
        filters='cssmin',
        output='gen/splash.css'
    ),

    "basic_table_css": flask_assets.Bundle(
        'css/bootstrap.min.css',
        'css/font-awesome.min.css',
        'css/datatables.min.css',
        'css/table-styling.css',
        filters='cssmin',
        output='gen/basic_table.css'
    ),

    "basic_table_js": flask_assets.Bundle(
        'js/pws.min.js', # persistent websocket https://github.com/porsager/pws
        'js/msgpack.min.js',
        'js/jquery-3.2.1.min.js',
        'js/datatables.min.js',
        'js/appUtil.js',
        output='gen/basic_table.js'
    )

}