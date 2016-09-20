#include <stdio.h>
#include <stdlib.h>

#include <dhashkit.h>

#define MODULA_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define MODULA_POINTS_PER_SERVER    1

uint32_t
modula_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *c;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    c = continuum + hash % ncontinuum;

    return c->index;
}
