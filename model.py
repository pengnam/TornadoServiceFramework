import arrow
import shortuuid

from dd.defined_dict import *
from dd.dd_cleaner import *

########################
##### BASE ###########
########################


class MapToMongoMixin(Mixin):

    @classmethod
    def map_to_mongo(cls, document):
        if document is None:
            return
        for key, definition in cls._fields.items():
            value = document.get(key)
            if value is not None:
                if isinstance(definition, DefinedDictField) and issubclass(definition.model, MapToMongoMixin):
                    definition.model.map_to_mongo(value)
                elif isinstance(definition, ListField) and isinstance(definition.inner_type, DefinedDictField):
                    for v in value:
                        definition.inner_type.model.map_to_mongo(v)
                else:
                    if hasattr(definition, "reversed_choices") and definition.reversed_choices is not None:
                        if isinstance(definition, ListField):
                            document[key] = [definition.choices.get(v) for v in value]
                        else:
                            document[key] = definition.choices.get(value)
                    if isinstance(definition, DateTimeField):
                        document[key] = arrow.get(
                            value).float_timestamp * DATETIME_STORE_PRECISION_V1  # store all datetime microseconds
                    if hasattr(definition, "store_field"):
                        document[definition.store_field] = document[key]
                        document.pop(key)

    @classmethod
    def map_from_mongo(cls, document):
        if document is None:
            return
        for key, definition in cls._fields.items():
            if hasattr(definition, "store_field") and definition.store_field in document:
                document[key] = document.pop(definition.store_field)
            value = document.get(key)
            if value is not None:
                if isinstance(definition, DefinedDictField) and issubclass(definition.model, MapToMongoMixin):
                    definition.model.map_from_mongo(value)
                elif isinstance(definition, ListField) and isinstance(definition.inner_type, DefinedDictField):
                    for v in value:
                        definition.inner_type.model.map_from_mongo(v)
                else:
                    if hasattr(definition, "reversed_choices") and definition.reversed_choices is not None:
                        if isinstance(definition, ListField):
                            document[key] = [definition.reversed_choices.get(v) for v in value]
                        else:
                            document[key] = definition.reversed_choices.get(value)
                    if isinstance(definition, DateTimeField):
                        document[key] = microsecond_to_datetime(document[key])


class BaseDocument(DefinedDict, MapToMongoMixin, CleanerMixin):
    pass


class BaseMongoDocument(BaseDocument):
    id = StringField(store_field="_id", default=lambda self: shortuuid.uuid(), labels=("restrict_input",))
    updated_at = DateTimeField(labels=("restrict_input", "restrict_update"))
    created_at = DateTimeField(labels=("restrict_update",))

    @classmethod
    def mark_timestamp(cls, document):
        now = arrow.utcnow().datetime
        document["updated_at"] = now
        document["created_at"] = document.get("created_at") or now
        return now

############################
### Models inherit from BaseDocument
#############################
