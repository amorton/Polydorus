from Crypto.Cipher import AES
import re

class EncryptedString(object):
    _value = None
    _bytes = None
    
    def __init__(self, secret, value=None, bytes=None):
        self.cipher = AES.new(secret)
        
        if bytes and value:
            raise Exception("Either value or bytes is required, but not both.")
        elif bytes:
            self.bytes = bytes
        elif value:
            self.value = value
        else:
            raise Exception("Either value or bytes is required.")
        
    @property
    def value(self):
        return self._value
        
    @property
    def bytes(self):
        return self._bytes
        
    @value.setter
    def value(self, value):
            self._value = value.decode('utf8')
            self._bytes = self.cipher.encrypt(value.encode('utf8').ljust(16))

    @bytes.setter
    def bytes(self, bytes):
        self._bytes = bytes
        self._value = self.cipher.decrypt(bytes).rstrip().decode('utf8')

    def __str__(self):
        return self._value

class PhoneNumber(object):
    _valid = False
    _number = None
    _country_code = None
    _area_code = None
    _local_number = None
    
    _re_non_digit = re.compile(r'[^\+0-9]')
    _re_valid_e164 = re.compile(r'^\+[1-9][0-9]{5,16}$')
    _re_011_number = re.compile(r'^011([2-9][0-9]{5,16})$')
    _re_non_nanp_local_number = re.compile(r'^0([1-9][0-9]{5,16})$')
    _re_non_nanp_int_number = re.compile(r'^00([1-9][0-9]{5,16})$')
            
    def __init__(self, input_value, default_country_code=None, validation_required=True):
        number = input_value
        if not self._re_valid_e164.match(number):
            number = self._re_non_digit.sub('', str(number).strip())
            if default_country_code == '1':
                if len(number) == 10:
                    number = '+1' + number
                elif self._re_011_number.match(number):
                    number = self._re_011_number.sub('+\\1', number)
            elif self._re_valid_e164.match('+' + number):
                number = '+' + number
            elif default_country_code is not None and self._re_non_nanp_local_number.match(number):
                number = '+' + default_country_code + self._re_non_nanp_local_number.sub('\\1', number)
            elif self._re_non_nanp_int_number.match(number):
                number = '+' + self._re_non_nanp_int_number.sub('\\1', number)

        if self._re_valid_e164.match(number):
            d1 = int(number[1:2])
            d2 = int(number[2:3])
            
            if d1 in (1, 7):
                cclen = 1
            elif (d1 == 2 and d2 not in (0, 7)) or \
                        (d1 == 3 and d2 in (5, 7, 8)) or \
                        (d1 == 4 and d2 == 2) or \
                        (d1 == 5 and d2 in (0, 9)) or \
                        (d1 == 6 and d2 >= 7) or \
                        (d1 == 8 and d2 in (0, 3, 5, 7, 8, 9)) or \
                        (d1 == 9 and d2 in (6, 7, 9)):
                cclen = 3
            else:
                cclen = 2
            
            cc = number[1:cclen+1]
            if cc=='1':
                if len(number) == 12:
                    self._country_code = cc
                    self._area_code = number[2:5]
                    self._local_number = number[5:]
                    self._number = number
                    self._valid = True
                else:
                    self._number = input_value
            else:
                self._country_code = cc
                self._local_number = number[cclen+1:]
                self._number = number
                self._valid = True
        else:
            self._number = input_value
    
    @property
    def valid(self):
        return self._valid
    
    @property
    def number(self):
        return self._number
        
    @property
    def parts(self):
        return (self._country_code, self._area_code, self._local_number)
        
    @property
    def friendly(self):
        if self._country_code == '1':
            return '(%s) %s-%s' % (self._country_code, self._area_code, self._local_number)
        else:
            return '+%s %s' % (self._country_code, self._local_number)
        
    def __str__(self):
        return self._number
