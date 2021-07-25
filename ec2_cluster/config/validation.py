from typing import Dict, List, Any

# TODO: these should be validation exceptions

def is_bool(field_name: str, field: Any) -> bool:
    return isinstance(field, bool)


def is_nonempty_string(field_name: str, field: Any) -> bool:
    if field is None:
        return False
    if not isinstance(field, str):
        return False

    return True


def is_nonempty_string_or_none(field_name: str, field: Any) -> bool:
    if field is None:
        return True
    return is_nonempty_string(field_name, field)


def is_positive_int(field_name: str, field: Any) -> bool:
    if field is None:
        return False
    if not isinstance(field, int):
        return False
    if field <= 0:
        return False
    return True

def is_positive_int_or_none(field_name: str, field: Any) -> bool:
    if field is None:
        return True
    return is_positive_int(field_name, field)

def validate_vpc(field_name: str, vpc: Any) -> bool:
    if not is_nonempty_string(field_name, vpc):
        return False
    if not vpc.startswith("vpc-"):
        return False

    return True


def validate_subnet(field_name: str, subnet: Any) -> bool:
    if not is_nonempty_string(field_name, subnet):
        return False
    if not subnet.startswith("subnet-"):
        return False

    return True


def validate_ami(field_name: str, ami: Any) -> bool:
    if not is_nonempty_string(field_name, ami):
        return False
    if not ami.startswith("ami-"):
        return False

    return True

def validate_security_groups(field_name: str, security_groups: Any) -> bool:
    # Note: empty list is fine
    if not isinstance(security_groups, List):
        return False
    for sg in security_groups:
        if not isinstance(sg, str):
            return False
        if not sg.startswith("sg-"):
            return False
    return True

# [
#     {
#         "Key": "some_user_defined_key",
#         "Value": "some_user_defined_value"
#     }
# ]
def validate_tags(field_name: str, tags: Any) -> bool:
    if not isinstance(tags, List):
        return False
    for tag in tags:
        if not isinstance(tag, Dict):
            return False
        if 'Key' not in tag.keys():
            return False
        if not isinstance(tag['Key'], str):
            return False
        if not len(tag['Key']) > 0:
            return False
        if 'Value' not in tag.keys():
            return False
        if not isinstance(tag['Value'], str):
            return False
        if not len(tag['Value']) > 0:
            return False
        if len(list(tag.keys())) > 2:
            return False

    return True