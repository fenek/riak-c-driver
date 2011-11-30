#include "urlcode.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

/* Converts a hex character to its integer value */
char from_hex(int ch) {
  return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
}

/* Converts an integer value to its hex character*/
char to_hex(int code) {
  static char hex[] = "0123456789abcdef";
  return hex[code & 15];
}

/* Returns a url-encoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *url_encode_bin(char *str, unsigned long strl) {
  unsigned char *pstr = (unsigned char *) str;
  char *buf = malloc(strl * 3 + 1);
  unsigned char *pbuf = (unsigned char *) buf;
  while (pstr < (unsigned char *)str + strl) {
    if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~') 
      *pbuf++ = *pstr;
    else 
      *pbuf++ = '%', *pbuf++ = to_hex(*pstr >> 4), *pbuf++ = to_hex(*pstr & 15);
    pstr++;
  }
  *pbuf = '\0';
  return buf;
}

char *url_encode(char *str) {
  unsigned char *pstr = (unsigned char *) str;
  char *buf = malloc(strlen(str) * 3 + 1);
  unsigned char *pbuf = (unsigned char *) buf;
  while (*pstr) {
    if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~') 
      *pbuf++ = *pstr;
    else 
      *pbuf++ = '%', *pbuf++ = to_hex(*pstr >> 4), *pbuf++ = to_hex(*pstr & 15);
    pstr++;
  }
  *pbuf = '\0';
  return buf;
}

/* Returns a url-decoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *url_decode(char *str) {
  unsigned char *pstr = str;
  char *buf = malloc(strlen(str) + 1);
  unsigned char *pbuf = buf;
  while (*pstr) {
    if (*pstr == '%') {
      if (pstr[1] && pstr[2]) {
        *pbuf++ = from_hex(pstr[1]) << 4 | from_hex(pstr[2]);
        pstr += 2;
      }
    } else if (*pstr == '+') { 
      *pbuf++ = ' ';
    } else {
      *pbuf++ = *pstr;
    }
    pstr++;
  }
  *pbuf = '\0';
  return buf;
}

/*
 * Local Variables:
 * indent-tabs-mode: nil
 * c-basic-offset: 2
 * End:
 */
