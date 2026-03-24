#include <cstdint>
#include <string>
#include <iostream>
#include <vector>

using namespace std;

int preserveUpperBits(int w, int n) {
  return (w >> (32 - n)) << (32 - n);
}

uint32_t string_to_ipv4(const string& str) {
  uint32_t ip = 0;
  uint32_t octet = 0;
  
  for (char c : str) {
    if (c == '.') {
      ip = (ip << 8) | octet;
      octet = 0;
    } else {
      octet = octet * 10 + (c - '0');
    }
  }
  
  return (ip << 8) | octet;  // Add final octet
}

string ipv4_to_string(uint32_t ip) {
  // TODO(human): Implement the reverse conversion
  // Extract octets and build dot-separated string
  vector<string> octets;
  
  for (int i = 0; i < 4; i++) {
    uint32_t octet = (ip >> (24 - i * 8)) & 0xFF;
    octets.push_back(to_string(octet));
  }
  
  string result = octets[0];
  for (int i = 1; i < 4; i++) {
    result += "." + octets[i];
  }
  
  return result;
}


int main() {
  cout << string_to_ipv4("192.168.1.1") << endl;

  uint32_t num = (3232235777 >> 24) & 0xFF;
  cout << num << endl;
  return 0;

}