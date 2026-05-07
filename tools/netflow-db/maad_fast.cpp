#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

constexpr double SpilloverThreshold = 0.05;
constexpr double DeltaQ = 1.0 / 8.0;
constexpr double MinQ = -0.5;
constexpr double MaxQ = 3.5;

struct PreparedMoment {
  std::vector<double> parent_counts;
  std::vector<std::pair<double, double>> child_counts;
};

uint32_t parse_ipv4(const std::string& text) {
  uint32_t value = 0;
  int octet = 0;
  int parts = 0;
  bool saw_digit = false;

  for (char ch : text) {
    if (ch >= '0' && ch <= '9') {
      saw_digit = true;
      octet = (octet * 10) + (ch - '0');
      if (octet > 255) {
        throw std::runtime_error("invalid IPv4 address");
      }
      continue;
    }
    if (ch == '.') {
      if (!saw_digit || parts >= 3) {
        throw std::runtime_error("invalid IPv4 address");
      }
      value = (value << 8) | static_cast<uint32_t>(octet);
      octet = 0;
      saw_digit = false;
      ++parts;
      continue;
    }
    if (ch == '\r' || ch == '\n' || ch == ' ' || ch == '\t') {
      continue;
    }
    throw std::runtime_error("invalid IPv4 address");
  }

  if (!saw_digit || parts != 3) {
    throw std::runtime_error("invalid IPv4 address");
  }
  return (value << 8) | static_cast<uint32_t>(octet);
}

uint32_t prefix_of(uint32_t address, int prefix_length) {
  if (prefix_length == 0) {
    return 0;
  }
  return address >> (32 - prefix_length);
}

std::vector<std::unordered_map<uint32_t, int>> build_prefix_counts(
    const std::vector<uint32_t>& addresses) {
  std::vector<std::unordered_map<uint32_t, int>> counts(33);
  for (int prefix_length = 0; prefix_length <= 32; ++prefix_length) {
    auto& by_prefix = counts[prefix_length];
    by_prefix.reserve(addresses.size());
    for (uint32_t address : addresses) {
      ++by_prefix[prefix_of(address, prefix_length)];
    }
  }
  return counts;
}

int first_atomic_length(const std::vector<std::unordered_map<uint32_t, int>>& counts) {
  for (int prefix_length = 1; prefix_length <= 32; ++prefix_length) {
    for (const auto& entry : counts[prefix_length]) {
      if (entry.second == 1) {
        return prefix_length;
      }
    }
  }
  return 33;
}

int first_spillover_length(const std::vector<std::unordered_map<uint32_t, int>>& counts) {
  for (int child_length = 1; child_length <= 32; ++child_length) {
    const double capacity = std::pow(2.0, 32 - child_length);
    for (const auto& entry : counts[child_length]) {
      if ((entry.second / capacity) >= (1.0 - SpilloverThreshold)) {
        return child_length;
      }
    }
  }
  return 33;
}

std::vector<PreparedMoment> prepare_moments(
    const std::vector<std::unordered_map<uint32_t, int>>& counts,
    int min_prefix_length,
    int max_prefix_length) {
  std::vector<PreparedMoment> prepared;
  for (int prefix_length = min_prefix_length; prefix_length <= max_prefix_length; ++prefix_length) {
    PreparedMoment moment;
    const auto& parents = counts[prefix_length];
    const auto& children = counts[prefix_length + 1];

    for (const auto& parent : parents) {
      if (parent.second <= 1) {
        continue;
      }
      const uint32_t left = parent.first << 1;
      const uint32_t right = left | 1U;
      auto left_it = children.find(left);
      auto right_it = children.find(right);
      if (left_it == children.end() && right_it == children.end()) {
        continue;
      }
      moment.parent_counts.push_back(static_cast<double>(parent.second));
      moment.child_counts.emplace_back(
          left_it == children.end() ? 0.0 : static_cast<double>(left_it->second),
          right_it == children.end() ? 0.0 : static_cast<double>(right_it->second));
    }
    prepared.push_back(std::move(moment));
  }
  return prepared;
}

std::pair<double, double> one_moment(const PreparedMoment& moment, double q) {
  if (moment.parent_counts.empty()) {
    return {0.0, 0.0};
  }

  std::vector<double> parent_powers;
  std::vector<double> child_power_sums;
  parent_powers.reserve(moment.parent_counts.size());
  child_power_sums.reserve(moment.child_counts.size());
  double this_z = 0.0;
  double next_z = 0.0;

  for (double count : moment.parent_counts) {
    const double power = std::pow(count, q);
    parent_powers.push_back(power);
    this_z += power;
  }
  for (const auto& child_pair : moment.child_counts) {
    double child_sum = 0.0;
    if (child_pair.first > 0.0) {
      child_sum += std::pow(child_pair.first, q);
    }
    if (child_pair.second > 0.0) {
      child_sum += std::pow(child_pair.second, q);
    }
    child_power_sums.push_back(child_sum);
    next_z += child_sum;
  }
  if (this_z <= 0.0 || next_z <= 0.0) {
    return {0.0, 0.0};
  }

  double d2 = 0.0;
  for (size_t index = 0; index < parent_powers.size(); ++index) {
    const double delta = (parent_powers[index] / this_z) - (child_power_sums[index] / next_z);
    d2 += delta * delta;
  }
  return {std::log2(this_z) - std::log2(next_z), d2};
}

std::vector<std::tuple<double, double, double>> compute_structure(
    const std::vector<PreparedMoment>& prepared) {
  std::vector<std::tuple<double, double, double>> rows;
  for (double q = MinQ; q <= MaxQ + 1e-9; q += DeltaQ) {
    double tau_sum = 0.0;
    double d2_sum = 0.0;
    for (const auto& moment : prepared) {
      const auto [tau, d2] = one_moment(moment, q);
      tau_sum += tau;
      d2_sum += d2;
    }
    const double n = static_cast<double>(prepared.size());
    rows.emplace_back(q, tau_sum / n, std::sqrt(d2_sum / n));
  }
  return rows;
}

std::vector<std::pair<double, double>> compute_spectrum(
    const std::vector<std::tuple<double, double, double>>& structure) {
  std::vector<std::pair<double, double>> alphas;
  for (size_t index = 1; index + 1 < structure.size(); ++index) {
    const double previous_tau = std::get<1>(structure[index - 1]);
    const double q = std::get<0>(structure[index]);
    const double tau = std::get<1>(structure[index]);
    const double next_tau = std::get<1>(structure[index + 1]);
    const double alpha = (next_tau - previous_tau) / (2.0 * DeltaQ);
    alphas.emplace_back(alpha, (q * alpha) - tau);
  }

  std::vector<std::pair<double, double>> rows;
  bool started = false;
  for (size_t index = 0; index + 1 < alphas.size(); ++index) {
    const bool decreasing = alphas[index].first > alphas[index + 1].first;
    if (!started && !decreasing) {
      continue;
    }
    if (!decreasing) {
      break;
    }
    started = true;
    rows.push_back(alphas[index + 1]);
  }
  return rows;
}

double info_dimension(
    const std::vector<std::unordered_map<uint32_t, int>>& counts,
    int min_prefix_length,
    int max_prefix_length,
    int total_addresses) {
  std::vector<std::pair<double, double>> points;
  const double total = static_cast<double>(total_addresses);
  for (int prefix_length = min_prefix_length; prefix_length <= max_prefix_length; ++prefix_length) {
    double entropy = 0.0;
    for (const auto& entry : counts[prefix_length]) {
      const double p = entry.second / total;
      entropy += p * std::log2(p);
    }
    points.emplace_back(-static_cast<double>(prefix_length), entropy);
  }

  double mean_x = 0.0;
  double mean_y = 0.0;
  for (const auto& point : points) {
    mean_x += point.first;
    mean_y += point.second;
  }
  mean_x /= points.size();
  mean_y /= points.size();

  double numerator = 0.0;
  double denominator = 0.0;
  for (const auto& point : points) {
    numerator += (point.first - mean_x) * (point.second - mean_y);
    denominator += (point.first - mean_x) * (point.first - mean_x);
  }
  return denominator == 0.0 ? 0.0 : numerator / denominator;
}

void write_number(std::ostream& out, double value) {
  if (std::isnan(value)) {
    out << "null";
    return;
  }
  out.precision(17);
  out << value;
}

void write_empty_json(int total_addresses) {
  std::cout << "{\"schemaVersion\":1,\"metadata\":{\"input\":\"-\",\"minPrefixLength\":null,"
               "\"maxPrefixLength\":null,\"totalAddrs\":"
            << total_addresses
            << "},\"structure\":[],\"spectrum\":[],\"dimensions\":[]}\n";
}

}  // namespace

int main(int, char**) {
  try {
    std::vector<uint32_t> addresses;
    std::string line;
    while (std::getline(std::cin, line)) {
      if (!line.empty()) {
        addresses.push_back(parse_ipv4(line));
      }
    }
    std::sort(addresses.begin(), addresses.end());
    addresses.erase(std::unique(addresses.begin(), addresses.end()), addresses.end());

    if (addresses.size() < 2) {
      write_empty_json(static_cast<int>(addresses.size()));
      return 0;
    }

    const auto counts = build_prefix_counts(addresses);
    const int min_prefix_length = first_atomic_length(counts);
    const int max_prefix_length = first_spillover_length(counts);
    if (min_prefix_length > max_prefix_length) {
      write_empty_json(static_cast<int>(addresses.size()));
      return 0;
    }

    const auto prepared = prepare_moments(counts, min_prefix_length, max_prefix_length);
    const auto structure = compute_structure(prepared);
    const auto spectrum = compute_spectrum(structure);
    const double dim_one = info_dimension(
        counts, min_prefix_length, max_prefix_length, static_cast<int>(addresses.size()));

    std::cout << "{\"schemaVersion\":1,\"metadata\":{\"input\":\"-\",\"minPrefixLength\":"
              << min_prefix_length << ",\"maxPrefixLength\":" << max_prefix_length
              << ",\"totalAddrs\":" << addresses.size() << "},\"structure\":[";
    for (size_t index = 0; index < structure.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      const auto [q, tau, sd] = structure[index];
      std::cout << "{\"q\":";
      write_number(std::cout, q);
      std::cout << ",\"tauTilde\":";
      write_number(std::cout, tau);
      std::cout << ",\"sd\":";
      write_number(std::cout, sd);
      std::cout << "}";
    }
    std::cout << "],\"spectrum\":[";
    for (size_t index = 0; index < spectrum.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      std::cout << "{\"alpha\":";
      write_number(std::cout, spectrum[index].first);
      std::cout << ",\"f\":";
      write_number(std::cout, spectrum[index].second);
      std::cout << "}";
    }
    std::cout << "],\"dimensions\":[{\"q\":1.0,\"dim\":";
    write_number(std::cout, dim_one);
    std::cout << "}";
    for (const auto& row : structure) {
      const double q = std::get<0>(row);
      if (std::abs(q) < 1e-12 || std::abs(q - 2.0) < 1e-12) {
        std::cout << ",{\"q\":";
        write_number(std::cout, q);
        std::cout << ",\"dim\":";
        write_number(std::cout, std::get<1>(row) / (q - 1.0));
        std::cout << "}";
      }
    }
    std::cout << "]}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "maad_fast failed: " << error.what() << "\n";
    return 1;
  }
}
