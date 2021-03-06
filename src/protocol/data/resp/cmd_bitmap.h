#pragma once

/**
 * create: create a bitmap of certain size, all bits reset unless value given
 * BitMap.create KEY size
 * Note: if size is not a multiple of the internal allocation unit (e.g. byte),
 * it will be rounded up internally
 * TODO: how to transfer value w/o being misrepresented due to endianness?
 * until we figure that out we shouldn't allow user to initialize w/ value
 *
 * delete: delete a bitmap
 * BitMap.delete KEY
 *
 * get: get value of a column in a bitmap
 * BitMap.get KEY columnId
 *
 * set: set value of a column in a bitmap
 * BitMap.set KEY columnId val
 */

/* TODO:
 * - variable-width columns. this PR will implement only 1-bit columns
 * - metadata: this will allow simple customization such as softTTL, timestamp
 *   or other info. This is the same idea behind memcached's `flag' field, but
 *   it's better to make it optional instead of allocating a fixed sized region
 *   for all commands and all data types.
 */

/*          type                string              #arg    #opt */
#define REQ_BITMAP(ACTION)                                      \
    ACTION( REQ_BITMAP_CREATE,  "BitMap.create",    3,      0  )\
    ACTION( REQ_BITMAP_DELETE,  "BitMap.delete",    2,      0  )\
    ACTION( REQ_BITMAP_GET,     "BitMap.get",       3,      0  )\
    ACTION( REQ_BITMAP_SET,     "BitMap.set",       4,      0  )

typedef enum bitmap_elem {
    BITMAP_KEY = 2,
    BITMAP_COL = 3,
    BITMAP_VAL = 4,
} bitmap_elem_e;
