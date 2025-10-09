"""
User ops processor for raw data.
"""

import polars as pl
from eth_abi import decode
import re
import regex
from web3 import Web3
w3 = Web3() # using offchain utils only

# Extract parameter types from a function signature
def extract_param_types_from_signature(signature):
    # remove the function name
    signature = signature[signature.find('(')+1:signature.rfind(')')]  # Get everything between the outer parentheses

    # split by commas but only if they are not in () brackets
    types = []
    current_type = ""
    paren_count = 0
    for char in signature:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            types.append(current_type.strip())
            current_type = ""
            continue
        current_type += char
    types.append(current_type.strip())

    # remove any substring that has exactly one whitespace left and a comma right from it
    for i, t in enumerate(types):
        while ' ' in t:
            index_whitespace = t.find(' ')
            index_comma = t.find(',', index_whitespace)
            index_bracket = t.find(')', index_whitespace)
            if index_whitespace != -1 and index_comma != -1:
                t = t[:index_whitespace] + t[index_comma:]
            elif index_whitespace != -1 and index_bracket != -1:
                t = t[:index_whitespace] + t[index_bracket:]
            elif index_whitespace != -1:
                t = t[:index_whitespace]
        types[i] = t

    return types

## custom parsing functions
def parse_custom_multisend(calldata: bytes):
    transactions = []
    offset = 0
    while offset < len(calldata):
        if offset + 85 > len(calldata):
            break
        # Extract fields: operation(1) + to(20) + value(32) + dataLength(32) + data
        operation = calldata[offset]
        to = calldata[offset+1:offset+21].hex()
        value = int.from_bytes(calldata[offset+21:offset+53], 'big')
        data_length = int.from_bytes(calldata[offset+53:offset+85], 'big')
        data = calldata[offset+85:offset+85+data_length]
        transactions.append((
            operation,
            f'0x{to}',
            value,
            data_length,
            data
        ))
        offset += 85 + data_length
    return (tuple(transactions),)

def parse_custom_remove_last_byte(data):
    if isinstance(data, bytes):
        return data[:-1] if len(data) > 0 else data
    elif isinstance(data, tuple):
        return tuple(parse_custom_remove_last_byte(item) for item in data)
    else:
        return data

def parse_custom_eip7821_batch(data):
    mode, executionCalldata = data

    def uint256(bytes, i): return int.from_bytes(bytes[i:i+32], 'big')
    def addr(bytes, i): return '0x' + bytes[i+12:i+32].hex()

    # mode
    final_dict = (
        (mode[:1], mode[1:2], mode[6:10].hex(), mode[10:]), # mode
    )

    # executionCalldata: first 32 bytes give the offset until trx_count
    offset_to_trx_count = uint256(executionCalldata, 0)

    if mode[:1] == b'\x01': # batch call = more than 1 transaction
        trx_count = uint256(executionCalldata, offset_to_trx_count) # trx_count
        # then for each transaction, 32 bytes for their offset follow
        trx_offsets = [uint256(executionCalldata, offset_to_trx_count+32*(i+1)) + offset_to_trx_count+32 for i in range(trx_count)]
        trx_data = []
        for i in range(trx_count):
            trx_data.append(executionCalldata[trx_offsets[i]:trx_offsets[i+1] if i+1 < trx_count else len(executionCalldata)])
    elif mode[:1] == b'\x00': # single call = 1 transaction
        trx_count = 1
        trx_data = [executionCalldata[offset_to_trx_count:]]
    else:
        raise ValueError("Unknown mode in EIP-7821 batchExecute.")

    # break down each transaction data into their components
    transaction_tuple = ()
    for trx in trx_data:
        target = addr(trx, 0)
        value = uint256(trx, 32)
        offset = uint256(trx, 64)
        calldata_len = uint256(trx, offset)
        calldata = trx[ offset+32 : offset+32+calldata_len ]
        if calldata_len == 0:
            pass # we do not count native transfers as a UserOp here
        elif calldata[-1] == 12:
            calldata = calldata[:-1] # remove last byte if it is 12 (padding byte?)
        if (len(calldata) - 4) % 32 != 0 and calldata_len != 0:
            calldata += b'\x00' * (32 - (len(calldata) - 4) % 32) # pad with zeros to make it a multiple of 32 bytes
        transaction_tuple += ((target, value, calldata_len, calldata),)
    final_dict += (transaction_tuple,)

    return final_dict

# Decode input bytes to a human-readable format using the pre-computed lookup map
def decode_input(input_bytes, four_byte_lookup: dict):
    # Extract method selector (first 4 bytes)
    method_selector = '0x' + input_bytes.hex()[:8]
    
    # !!! Edge cases for custom calldata structure such as Gnosis Safe multiSend(bytes transactions) on Worldchain
    try:
        if method_selector == '0x8d80ff0a': # Gnosis Safe multi send
            true_signature = "multiSend(bytes transactions)"
            custom_signature = "multiSend((uint8 operation,address to,uint256 value,uint256 dataLength,bytes data)[] transactions)"
            true_decoded = decode(extract_param_types_from_signature(true_signature), input_bytes[4:])[0]
            custom_decoded = parse_custom_multisend(true_decoded)
            return custom_decoded, custom_signature
        elif method_selector == '0x99e1d016': # Uniswap EIP7702 execute #1
            true_signature = "execute(((address,uint256,bytes)[] calls,bool revertOnFailure) batchedCall)"
            true_decoded = decode(extract_param_types_from_signature(true_signature), input_bytes[4:])
            custom_decoded = parse_custom_remove_last_byte(true_decoded)
            return custom_decoded, true_signature
        elif method_selector == '0x3593564c': # Uniswap EIP7702 execute #2
            true_signature = "execute(bytes commands,bytes[] inputs,uint256 deadline)"
            custom_signature = "execute(uint256 commands,bytes[] inputs,uint256 deadline)"
            custom_decoded = decode(extract_param_types_from_signature(custom_signature), input_bytes[4:])
            return custom_decoded, custom_signature
        elif method_selector == '0xe9ae5c53': # EIP7821 batch execute(bytes32 mode,bytes executionCalldata)
            true_signature = "batchExecute(bytes32 mode,bytes executionCalldata)"
            custom_signature = "batchExecute((bytes1 CallType,bytes1 ExecType,string ExecType,bytes22 ModePayload) mode,(address target,uint256 value,uint256 calldataLength,bytes calldata)[] transactions)"
            true_decoded = decode(extract_param_types_from_signature(true_signature), input_bytes[4:])
            #print(f"true_decoded: {true_decoded}")
            custom_decoded = parse_custom_eip7821_batch(true_decoded)
            return custom_decoded, custom_signature
        elif method_selector == '0x8f0273a9': # we do not have this method in our 4bytes.parquet file as of Sep.4th2025, but it is in https://www.4byte.directory/
            true_signature = "batchCall((address target,bool allowFailure,uint256 value,bytes data)[] transactions)"
            true_decoded = decode(extract_param_types_from_signature(true_signature), input_bytes[4:])
            return true_decoded, true_signature
    except Exception as e:
        print(f"Failed to decode custom method {method_selector}: {str(e)}")

   # Using the dictionary four_byte_lookup.pkl for a high-speed O(1) lookup
    signatures = four_byte_lookup.get(method_selector)

    # if no methods are found, return
    if not signatures:
        return None, None

    # Try to decode the input data using the method signature(s)
    for signature in signatures:
        try:
            types = extract_param_types_from_signature(signature)
            calldata = input_bytes[4:]
            if not types and not calldata:
                # special case for function calls without input bytes, return empty dict with signature
                return (), signature
            d = decode(types, calldata)
            return d, signature
        except Exception as e:
            pass
            #print(f"Failed to decode using method {method['signature']}: {str(e)}")

    return None, None

# returns dict with extracted bytes and array index as key
def extract_calldata_bytes_recursive(data):
    if isinstance(data, bytes):
        if (len(data) - 4) % 32 == 0:  # Only return bytes that have the correct calldata length (calldata length is 4 + 32n)
            return data
        else:
            return None
    elif isinstance(data, tuple):
        result = {}
        i = 0 # creates a dict with the array index as keys
        for item in data:
            if (extracted := extract_calldata_bytes_recursive(item)) is not None:
                result[i] = extracted
            i += 1
        # remove empty keys {}
        result = {k: v for k, v in result.items() if v != {}}
        return result
    else:
        return None

# returns true if bytes in data and at least 4 bytes long
def extract_has_bytes(data):
    if isinstance(data, bytes) and len(data) > 3:
        return True
    elif isinstance(data, tuple):
        return any(extract_has_bytes(item) for item in data)
    return False

# flatten result from extract_calldata_bytes_recursive into a single level dict
def flatten_byte_dict(d):
    if not isinstance(d, dict):
        return d
    # If this dict has only one key, unwrap it
    if len(d) == 1:
        key, value = next(iter(d.items()))
        # Recursively flatten the value
        return flatten_byte_dict(value)
    # If this dict has multiple keys, recursively flatten each value
    result = {}
    for key, value in d.items():
        result[key] = flatten_byte_dict(value)
    return result

# get the hierarchy of keys of a nested dict
def get_hierarchy_keys(d, level=0, all_levels=None):
    if all_levels is None:
        all_levels = []
    
    # Extend all_levels to have enough sets for current level
    while len(all_levels) <= level:
        all_levels.append(set())
    
    # Add current level keys (set automatically handles duplicates)
    all_levels[level].update(d.keys())
    
    # Recurse into nested dicts
    for value in d.values():
        if isinstance(value, dict):
            get_hierarchy_keys(value, level + 1, all_levels)
    
    # Convert sets back to lists
    return [list(level_set) for level_set in all_levels]

# function to check if a hierarchy is a branch and should be split
def is_branch(hierarchy):
    # we do 2 checks here to see if we should split the branch:
    # 1) is only one of the levels a list?
    # 2) for the list level, does it start from 0 or 1?
    number_of_lists = 0
    start_from_zero_or_one = False
    # Check if the key exists at any level in the hierarchy
    for level in hierarchy:
        if len(level) > 1:
            number_of_lists += 1
            if level[0] == 0 or level[0] == 1:
                start_from_zero_or_one = True

    # If we have exactly one list and it starts from zero, we have a branch
    return number_of_lists == 1 and start_from_zero_or_one

# decodes any input bytes, as far down as possible
def process_transaction(trx, four_byte_lookup: dict):
    
    unchecked = []
    checked = []

    # only process input if the calldata length is a valid function call: 4 + 32*n bytes
    if (len(trx.input) - 4) % 32 == 0:
        d, sig = decode_input(trx.input, four_byte_lookup)

        if d is not None:
            unchecked.append(
                {
                    "signature": sig,
                    "decode_input": d,
                    "index": '0',
                    "tree": sig.split('(')[0] if '(' in sig else sig,
                    "tree_4byte": trx.input.hex()[:8] if hasattr(trx.input, 'hex') else f"ERROR_input_is_{type(trx.input)}",
                    "is_decoded": True, # was the function call decoded?
                }
            )

    while unchecked != []:
        #print(f"Processing unchecked items: {len(unchecked)}")
        item = unchecked.pop(0)
        sig = item['signature']
        data = item['decode_input']
        index = item['index']
        tree = item['tree']
        tree_4byte = item['tree_4byte']
        is_decoded = item['is_decoded']

        #print(f"looking for bytes in: {data}")
        byte_dict = extract_calldata_bytes_recursive(data)

        # found no calldata bytes
        if byte_dict == {} or byte_dict is None:
            pass
            #print("Found no bytes.")

        # found one calldata byte
        elif isinstance(byte_dict, bytes):
            #print(f"Found one byte: {byte_dict.hex()}")
            d2, sig2 = decode_input(byte_dict, four_byte_lookup)
            #print(f"|__ d2 data: {d2}")
            #print(f"|__ sig2 data: {sig2}")
            if d2 != None: # successful decoding
                #print(f"|__ decoded {sig2} with index {index}:{0}, adding to unchecked list")
                unchecked.append({
                    "signature": sig2,
                    "decode_input": d2,
                    "index": f"{index}:{0}",
                    "tree": f"{tree}:{sig2.split('(')[0] if '(' in sig2 else sig2}",
                    "tree_4byte": f"{tree_4byte}:{byte_dict.hex()[:8]}",
                    "is_decoded": True
                })
            else: # unsuccessful decoding
                #print(f"|__ unsuccessful decoded {byte_dict.hex()[:8]} with index {index}:{0}, adding to unchecked list")
                unchecked.append({
                    "signature": None,
                    "decode_input": None,
                    "index": f"{index}:{0}",
                    "tree": f"{tree}:{byte_dict.hex()[:8]}",
                    "tree_4byte": f"{tree_4byte}:{byte_dict.hex()[:8]}",
                    "is_decoded": False
                })
        elif isinstance(flatten_byte_dict(byte_dict), bytes):
            byte_dict_flattend = flatten_byte_dict(byte_dict)
            #print(f"Found one byte in array: {byte_dict_flattend.hex()}")
            d2, sig2 = decode_input(byte_dict_flattend, four_byte_lookup)
            #print(f"|__ d2 data: {d2}")
            #print(f"|__ sig2 data: {sig2}")
            if d2 != None: # successful decoding
                #print(f"|__ decoded {sig2} with index {index}:{0}, adding to unchecked list")
                unchecked.append({
                    "signature": sig2,
                    "decode_input": d2,
                    "index": f"{index}:{0}",
                    "tree": f"{tree}:{sig2.split('(')[0] if '(' in sig2 else sig2}",
                    "tree_4byte": f"{tree_4byte}:{byte_dict_flattend.hex()[:8]}",
                    "is_decoded": True
                })
            else: # unsuccessful decoding
                #print(f"|__ unsuccessful decoded {byte_dict_flattend.hex()[:8]} with index {index}:{0}, adding to unchecked list")
                unchecked.append({
                    "signature": None,
                    "decode_input": None,
                    "index": f"{index}:{0}",
                    "tree": f"{tree}:{byte_dict_flattend.hex()[:8]}",
                    "tree_4byte": f"{tree_4byte}:{byte_dict_flattend.hex()[:8]}",
                    "is_decoded": False
                })
                
        # found multiple calldata bytes, this will cause a split
        elif isinstance(byte_dict, dict):
            #print(f"Found multiple bytes in array")
            # get the hierarchy of where the bytes are stored in the dict
            hierarchy = get_hierarchy_keys(byte_dict)
            if is_branch(hierarchy):
                #print(f"|__ is_branch resolved to True for hierarchy: {hierarchy}")
                byte_dict_flattend = flatten_byte_dict(byte_dict)
                for i, byte in byte_dict_flattend.items():
                    #print(f"Decoding byte {i+1}/{len(byte_dict_flattend)} with array index {i}")
                    d2, sig2 = decode_input(byte, four_byte_lookup)
                    #print(f"|__ d2 data: {d2}")
                    #print(f"|__ sig2 data: {sig2}")
                    if d2 != None: # successful decoding
                        #print(f"|__ decoded {sig2} with index {index}:{i}, adding to unchecked list")
                        unchecked.append({
                            "signature": sig2,
                            "decode_input": d2,
                            "index": f"{index}:{i}",
                            "tree": f"{tree}:{sig2.split('(')[0] if '(' in sig2 else sig2}",
                            "tree_4byte": f"{tree_4byte}:{byte.hex()[:8]}",
                            "is_decoded": True
                        })
                    else: # unsuccessful decoding
                        #print(f"|__ unsuccessful decoded {byte.hex()[:8]} with index {index}:{0}, adding to unchecked list")
                        unchecked.append({
                            "signature": None,
                            "decode_input": None,
                            "index": f"{index}:{i}",
                            "tree": f"{tree}:{byte.hex()[:8]}",
                            "tree_4byte": f"{tree_4byte}:{byte.hex()[:8]}",
                            "is_decoded": False
                        })
            else:
                pass
                #print(f"|__ is_branch resolved to False for hierarchy: {hierarchy}")

        checked.append({
            "signature": sig,
            "decode_input": data,
            "index": index,
            "tree": tree,
            "tree_4byte": tree_4byte,
            "is_decoded": is_decoded,
            "has_bytes": extract_has_bytes(data)
        })

    return checked

### Matching trees together functions 

# finds the end nodes of each user operation tree
def find_tree_ends(str_nodes: list[str]):
    ends = []
    for node in str_nodes:
        # Check if this node is a parent of any other node
        is_parent = any(other.startswith(node + ':') for other in str_nodes if other != node)
        # If it's not a parent, it's a leaf/end node
        if not is_parent:
            ends.append(node)
    return ends

# finds the correct index for the tuple in the tree_end
def get_next_index(index, tree_end):
    i = tree_end.find(index) + len(index)
    return int(tree_end[i+1:].split(':')[0] if i != -1 else None)

# extracts the type names from a function signature
def extract_type_names(sig):
    type_str = sig[sig.find('(')+1:sig.rfind(')')]
    # replace everything inside brackets '(...)' with an empty string ''
    type_str = regex.sub(r'\((?:[^()]++|(?R))*+\)', '', type_str)
    # remove any []
    type_str = re.sub(r'\[\]', '', type_str)
    # split into a list
    type_str = type_str.split(' ')
    # only keep types which have a ,
    types = [t for t in type_str if ',' in t]
    # only keep everything before the comma for each type
    types = [t.split(',')[0] for t in types]
    # add the last type
    types.append(type_str[-1]) if type_str else None
    # remove empty strings
    types = [t for t in types if t]
    return types

# matches the decoded data to the type list of extract_type_names
def match_types_to_data(decode_data, type_list):
    #print(f"Matching types: {type_list} with data: {decode_data}")
    if len(decode_data) != len(type_list):
        raise ValueError(f"Tuple length ({len(decode_data)}) must match type_list length ({len(type_list)})")
    return dict(zip(type_list, decode_data))

# find the matching opening bracket position
def find_matching_opening_bracket(sig, key_with_closing_bracket):
    # Find the position of key_with_closing_bracket e.g. ') {key}'
    key_pos = sig.find(key_with_closing_bracket)
    if key_pos == -1:
        return -1, -1
    # The closing bracket is at key_pos (the ')' character)
    closing_pos = key_pos
    # Go backwards from the closing bracket to find its matching opening bracket
    bracket_count = 1  # We start with 1 because we found a closing bracket
    pos = closing_pos - 1
    while pos >= 0:
        if sig[pos] == ')':
            bracket_count += 1  # Found another closing bracket
        elif sig[pos] == '(':
            bracket_count -= 1  # Found an opening bracket
            if bracket_count == 0:  # Found the matching opening bracket
                return pos, closing_pos
        pos -= 1
    return -1, -1  # No matching bracket found

# extracts the correct information from the decoded data based on the tree end (can only detect params one tuple deep, e.g. decreaseLiquidity((uint256 tokenId,uint128 liquidity,uint256 amount0Min,uint256 amount1Min,uint256 deadline) params))
def extract_information(data, tree_end):
    decode_data = data['decode_input']
    # if tuple, start iteration
    if isinstance(decode_data, tuple):
        sig = data['signature']
        #print(f"Processing tuple with signature {sig} and index {data['index']}")
        info_dict = match_types_to_data(decode_data, extract_type_names(sig))
        #print(f"info dict: {info_dict}")
        for key, d in info_dict.items():
            if isinstance(d, tuple):
                # if we are not at the end of the tree, pick the correct index of the tuple
                if data['index'] != tree_end:
                    if f')[] {key},' in sig or f')[] {key})' in sig: # note: such arrays will be ignored "uint256[2] _deposit_limit"
                        # select the correct index to get the information from e.g. handleOps((...)[] ops,address beneficiary)
                        i = get_next_index(data['index'], tree_end)
                        if f')[] {key},' in sig:
                            opening_pos, closing_pos = find_matching_opening_bracket(sig, f')[] {key},')
                        else:
                            opening_pos, closing_pos = find_matching_opening_bracket(sig, f')[] {key})')
                        try:
                            info_dict[key] = match_types_to_data(d[i], extract_type_names(sig[opening_pos:closing_pos + 1]))
                        except Exception as e:
                            pass
                            #print(f"WARNING matching types for {key}: {e}")
                    elif f'[] {key},' in sig or f'[] {key})' in sig:
                        # select the correct index but no further tuples present e.g. multicall(bytes[] data)
                        i = get_next_index(data['index'], tree_end)
                        try:
                            info_dict[key] = d[i]
                        except Exception as e:
                            pass
                            #print(f"WARNING matching types for {key}: {e}")
                    elif f') {key},' in sig or f') {key})' in sig:
                        # no array, just a tuple present e.g. execute(((address,uint256,bytes)[] calls,bool revertOnFailure) batchedCall)
                        if f') {key},' in sig:
                            opening_pos, closing_pos = find_matching_opening_bracket(sig, f') {key},')
                        else:
                            opening_pos, closing_pos = find_matching_opening_bracket(sig, f') {key})')
                        try:
                            info_dict[key] = match_types_to_data(d, extract_type_names(sig[opening_pos:closing_pos + 1]))
                        except Exception as e:
                            pass
                            #print(f"WARNING matching types for {key}: {e}")
                elif f') {key},' in sig or f') {key})' in sig:
                    # extract and match subtype names if there are non array tuples e.g. collect((uint256 tokenId,address recipient,uint128 amount0Max,uint128 amount1Max) params)
                    if f') {key},' in sig:
                        opening_pos, closing_pos = find_matching_opening_bracket(sig, f') {key},')
                    else:
                        opening_pos, closing_pos = find_matching_opening_bracket(sig, f') {key})')
                    info_dict[key] = match_types_to_data(d, extract_type_names(sig[opening_pos:closing_pos + 1]))
                else:
                    # if we are at the end of the tree, just return the data
                    info_dict[key] = d

    elif decode_data == {}:
        # if decode_data is empty, we return an empty dict
        info_dict = {}
    else:
        # in case of a single value, we just return the value
        info_dict[sig[sig.find('(')+1:sig.rfind(')')]] = decode_data

    return info_dict

# turn decoded_list into a list with tree ends and all decode_input as values
def match_transaction(decoded_list):
    f = []
    for tree_end in find_tree_ends([item['index'] for item in decoded_list]):
        #print(f"Processing tree end: {tree_end}")
        record = {
            "full_decoded_input": {},
            "index": tree_end
        }
        for data in decoded_list:
            if tree_end.startswith(data['index']) or data['index'] == tree_end:
                #print(f"Found match for {data['index']} with tree_end: {tree_end}")
                if data['signature'] != None:
                    ext = extract_information(data, tree_end)
                    record["full_decoded_input"][data['index']] = ext # sorted by index as key
                # Add 'tree', 'tree_4byte', 'is_decoded' & 'has_bytes' key and value if index matches exactly
                if data['index'] == tree_end and 'tree' in data:
                    record['tree'] = data['tree']
                    record['tree_4byte'] = data['tree_4byte']
                    record['is_decoded'] = data['is_decoded']
                    record['has_bytes'] = data['has_bytes']
        f.append(record)
    
    return f

### extracting information from dict

# nested search
def search_nested_dict(data, options):
    """Recursively search for 'to' or 'target' keys in nested dictionaries"""
    if isinstance(data, dict):
        # First check direct keys
        for option in options:
            if option in data: #and isinstance(data[option], str):
                return data[option]
        # Then recursively search nested dictionaries
        for value in data.values():
            result = search_nested_dict(value, options)
            if result:
                return result
    return None

# find to address, in reverse tree order until first match is found
functions_to = '*' # any function
options_to = ['to', '_to', 'target', 'targets', 'dest', 'targetContract']
def find_to_address(f, functions, options):
    
    # return if no userops exist
    if f['full_decoded_input'] is None:
        return None
    
    # search all function names
    if functions == '*':
        all_index = f['full_decoded_input'].keys()
        all_index = list(reversed(all_index))
        # remove the last index (just a function call), in case it exist in all_index
        if f['index'] in all_index:
            all_index.remove(f['index'])
        for index in all_index:
            result = search_nested_dict(f['full_decoded_input'][index], options)
            if w3.is_address(result):
                #print(f"to address found: {result} in {index}")
                return result

    # go through each function in functions and search for options
    else:
        tree_functions = f['tree'].split(':')
        for function in functions:
            function_indices = [i for i, x in enumerate(tree_functions) if x == function]
            function_indices = list(reversed(function_indices))
            for i in function_indices:
                specific_index = ':'.join(f['index'].split(':')[:i + 1])
                result = search_nested_dict(f['full_decoded_input'][specific_index], options)
                if w3.is_address(result):
                    #print(f"to address found: {result} in {specific_index}")
                    return result

    return None

# find from address, in tree order until first match is found
functions_from = ['handleOps', 'execTransaction']
options_from = ['sender', 'from', '_from', 'origin']
def find_from_address(f, functions, options):

    # return if no userops exist
    if f['full_decoded_input'] is None:
        return None

    # search all function names
    if functions == '*':
        all_index = f['full_decoded_input'].keys()
        # remove the last index (just a function call), in case it exist in all_index
        if f['index'] in all_index:
            all_index.remove(f['index'])
        for index in all_index:
            result = search_nested_dict(f['full_decoded_input'][index], options)
            if w3.is_address(result):
                #print(f"from address found: {result} in {index}")
                return result

    # go through each function in functions and search for options
    else:
        tree_functions = f['tree'].split(':')
        for function in functions:
            function_indices = [i for i, x in enumerate(tree_functions) if x == function]
            for i in function_indices:
                specific_index = ':'.join(f['index'].split(':')[:i + 1])
                result = search_nested_dict(f['full_decoded_input'][specific_index], options)
                if w3.is_address(result):
                    #print(f"from address found: {result} in {specific_index}")
                    return result
                else:
                    #print('special case found')
                    # if options not found, look for to address in parent (e.g. 'tree': 'aggregate3:aggregate3:execTransaction:aggregate3:approve')
                    parent_index = ':'.join(f['index'].split(':')[:i])
                    if parent_index != '':
                        result = search_nested_dict(f['full_decoded_input'][parent_index], ['to', 'target'])
                        if w3.is_address(result):
                            #print(f"to address found: {result} in {parent_index}")
                            return result

    return None

# find ERC4337 information, in tree order until first match is found
functions_ERC4337 = ['handleOps']
options_beneficiary = ['beneficiary']
options_paymaster = ['paymasterAndData']
def find_ERC4337_information(f, functions, options_beneficiary, options_paymaster):

    # return if no userops exist
    if f['full_decoded_input'] is None:
        return None, None

    beneficiary = None
    paymaster = None

    # search all function names
    if functions == '*':
        all_index = f['full_decoded_input'].keys()
        for index in all_index:
            beneficiary = search_nested_dict(f['full_decoded_input'][index], options_beneficiary)
            paymasterAndData = search_nested_dict(f['full_decoded_input'][index], options_paymaster)
            paymaster = '0x' + paymasterAndData.hex()[:40] if paymasterAndData else None # can be None if no paymaster was used
            if w3.is_address(beneficiary) or w3.is_address(paymaster):
                #print(f"beneficiary address found: {beneficiary} in {index}")
                #print(f"paymaster address found: {paymaster} in {index}")
                return beneficiary, paymaster

    # go through each function in functions and search for options
    else:
        tree_functions = f['tree'].split(':')
        for function in functions:
            function_indices = [i for i, x in enumerate(tree_functions) if x == function]
            for i in function_indices:
                specific_index = ':'.join(f['index'].split(':')[:i + 1])
                beneficiary = search_nested_dict(f['full_decoded_input'][specific_index], options_beneficiary)
                paymasterAndData = search_nested_dict(f['full_decoded_input'][specific_index], options_paymaster)
                paymaster = '0x' + paymasterAndData.hex()[:40] if paymasterAndData else None # can be None if no paymaster was used
                if w3.is_address(beneficiary) or w3.is_address(paymaster):
                    #print(f"beneficiary address found: {beneficiary} in {specific_index}")
                    #print(f"paymaster address found: {paymaster} in {specific_index}")
                    return beneficiary, paymaster
    
    return None, None

### main function
def find_UserOps(trx, four_byte_lookup: dict):
    try:
        #print(f"### Processing transaction {trx.hash.hex()}")
        j = process_transaction(trx, four_byte_lookup)
        #print(f"### Done decoding, total UserOps decoded: {len(j)}")
        f = match_transaction(j)
        #print(f"### Done matching")
        for uo in f:
            if uo['full_decoded_input'] is not None:
                uo['from'] = find_from_address(uo, functions_from, options_from)
                uo['to'] = find_to_address(uo, functions_to, options_to)
                uo['beneficiary'], uo['paymaster'] = find_ERC4337_information(uo, functions_ERC4337, options_beneficiary, options_paymaster)
            else:
                uo['from'] = None
                uo['to'] = None
                uo['beneficiary'] = None
                uo['paymaster'] = None
        #print(f"### Done extracting information\n")
        # remove full_decoded_input to save space, then add hash and blockNumber
        for uo in f:
            uo.pop('full_decoded_input', None)
            try:
                uo['hash'] = '0x' + trx.hash.hex()
            except AttributeError as e:
                # Fallback for string hash
                if isinstance(trx.hash, str):
                    uo['hash'] = trx.hash if trx.hash.startswith('0x') else '0x' + trx.hash
                else:
                    raise e
            uo['blockNumber'] = trx.blockNumber
        ### we only save the UserOp has at least one property of the following:
        # a 'from', 'to', 'paymaster' or 'beneficiary' address was decoded
        # there are multiple operations (e.g. multisend) 
        # has_bytes is True
        if (
            any(uo['from'] != None for uo in f) 
            or any(uo['to'] != None for uo in f) 
            or any(uo['paymaster'] != None for uo in f) 
            or any(uo['beneficiary'] != None for uo in f) 
            or len(f) > 1 
            or any(uo['has_bytes'] == True for uo in f)
        ):
            # if from or to is None, we default back to the original trx from/to
            for uo in f:
                if uo['from'] is None:
                    # print("from address not found, defaulting back to original from")
                    uo['from'] = trx.fromAddress
                if uo['to'] is None:
                    # print("to address not found, defaulting back to original to")
                    uo['to'] = trx.toAddress
            return f
        else:
            return []
    except Exception as e:
        print(f"Error processing transaction {trx.hash.hex()}: {str(e)}")
        return []
