/********************************************************************

  Copyright 2012 Konstantin Olkhovskiy <lupus@oxnull.net>

  This file is part of Mersenne.

  Mersenne is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  any later version.

  Mersenne is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Mersenne.  If not, see <http://www.gnu.org/licenses/>.

 ********************************************************************/
#ifndef _SHAREDMEM_H_
#define _SHAREDMEM_H_

typedef void(*sm_destructor_t)(void *context, void *ptr);
void * sm_alloc(size_t size);
void * sm_alloc_ext(size_t size, sm_destructor_t destructor, void *context);
void * sm_calloc(size_t nmemb, size_t size);
void * sm_calloc_ext(size_t nmemb, size_t size, sm_destructor_t destructor, void *context);
void sm_set_destructor(void *ptr, sm_destructor_t destructor, void *context);
size_t sm_size(void *ptr);
void * sm_in_use(void *ptr);
void sm_free(void *ptr);

#endif
