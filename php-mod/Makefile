PHP_CONFIG ?= php-config
CC ?= gcc

PHP_MAJOR := $(shell $(PHP_CONFIG) --version | cut -d. -f1)
CFLAGS := $(CFLAGS) $(shell $(PHP_CONFIG) --includes) -O2 -fPIC -Wall -Wextra

ifeq ($(PHP_MAJOR),7)
	PHP_LIB := -lphp7
else
	PHP_LIB := -lphp
endif

LDFLAGS := $(LDFLAGS) $(shell $(PHP_CONFIG) --ldflags) $(PHP_LIB)

TARGET := libproteus-php-mod.so

.PHONY: all clean
all: $(TARGET)

$(TARGET): src/proteus_php_mod.c src/proteus_php_mod.h
	$(CC) $(CFLAGS) -shared -o $@ src/proteus_php_mod.c $(LDFLAGS)

clean:
	rm -f $(TARGET)
