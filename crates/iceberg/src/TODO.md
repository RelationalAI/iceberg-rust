TODO @vustef:
- don't make new_input async. We don't need to refresh cred during create_operator.
- instead we can create new opendal operator (rather accessor, that will be the refreshable storage), and implement the API for opendal, to refresh for each call.
- the refresh will not really happen always, it will depend on the loader
- the refresh function should signal whether it refreshed. When refreshed, it will create the inner opendal operator (for actual inner storage), and delegate to that one. Otherwise just delegate to existing opendal operator for inner storage.
- we can also retry on expiration failure, up to once, if we can detect this