

def string_to_address(address):
    """
    Change string representation of server address, e.g. 127.0.0.1:9999 to host, port tuple needed for socket API.

    >>> string_to_address('127.0.0.1:9999')
    ('127.0.0.1', 9999)
    """
    addr = address.split(":")
    host = addr[0]
    port = int(addr[1])
    return host, port


def address_to_node_id(servers, address):
    return servers.index(address)
