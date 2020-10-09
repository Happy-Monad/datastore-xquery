from google.cloud import datastore

class Xquery():
    '''
    Allows to make Datastore queries that are not supported by Google's Datastore library.
        - Queries with 1 range and 1 >= equality filters without composite indexes.
        - Queries with multiple range filters.
        - Queries with a range filter and sorted first by another property
        - Any combination of the above
    '''

    def __init__(self, kind, client=None):
        '''
        Instantiates the query creator object with given kind.
        If Datastore client object is not supplied a new instance is created.
        ''' 
        self.client = client or datastore.Client()
        self.kind = kind
        self._filters = {}
        self._order = []
    
    def add_filter(self, property_name, operator, value):
        '''
        Adds a property filter to the query.
        '''
        if self._filters.get(property_name):
            self._filters[property_name].append((property_name, operator, value))
        else:
            self._filters[property_name] = [(property_name, operator, value)]
    
    def order(self, value):
        '''
        Add sort order in one or several properties.
        Properties may be sorted arbitrarely regardless of range filters.
        '''
        if isinstance(value, str):
            value = [value]
        self._order = value
    
    def _fetch_keys(self, clauses):
        '''
        Fetches the keys fulfilling the filter clauses.
        '''
        query = self.client.query(kind=self.kind)
        query.keys_only()
        for clause in clauses:
            query.add_filter(*clause)
        return (x.key for x in query.fetch())

    def fetch(self):
        '''
        Returns a list of Datastore entities with filters and sort orders applied.
        '''
        if not self.kind:
            raise ValueError("An entity kind must be specified.")
        
        if not len(self._filters):
            raise ValueError("At least one filter clause is needed.")
        
        it = iter(self._filters)
        keys = set(self._fetch_keys(self._filters[next(it)]))
        
        for field in it:
            keys.intersection_update(self._fetch_keys(self._filters[field]))
        
        results = list(self.client.get_multi(keys))

        if self._order:
            for field in reversed(self._order):
                reverse = True if field[0] == '-' else False 
                field_name =  field if field[0] != '-' else field[1:]
                results.sort(key=lambda x: x[field_name], reverse=reverse)
        
        return results

    def clear(self):
        '''
        Clears query filters data to allow object reuse.
        '''
        self._filters = {}
        self._order = []
        self.kind = None


if __name__ == '__main__':
    # Example usage
    # 1 range + 1 or more euality + no composite index 
    c = Xquery('fields')
    c.add_filter('f1', '<=', 2)
    c.add_filter('f2', '=', 2)
    c.add_filter('f3', '=', 2)
    c.fetch()
    c.clear()

    # More than 1 range filters
    c.add_filter('f1', '<=', 2)
    c.add_filter('f2', '>=', 2)
    c.add_filter('f3', '=', 2)
    c.fetch()
    c.clear()

    # Arbitrary sort order
    c.add_filter('f1', '<=', 2)
    c.order('f2')
    c.fetch()
    c.clear()
