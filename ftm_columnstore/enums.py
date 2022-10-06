from followthemoney import model

SCHEMATA = set(model.schemata.keys())
PROPERTIES = set([p.name for p in model.properties])
PROPERTIES_FPX = set([p.name for p in model.properties if p.type.name == "name"])
PROPERTY_TYPES = set(p.type.name for p in model.properties)
