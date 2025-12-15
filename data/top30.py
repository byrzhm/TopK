from collections import Counter

def is_private_ip(ip):
    """Check if the IP starts with 10. or 127."""
    return ip.startswith('10.') or ip.startswith('127.')

def parse_apache_log_line(line: str):
    """Extract IP and status code from a log line using regex."""
    # Regex to match the IP and status code in Apache common log format
    ip = line.split(" ")[0]
    status = line.split(" ")[-2]
    return ip, status

def get_top_ips(log_file_path, top_n=30):
    ip_counter = Counter()

    with open(log_file_path, 'r') as file:
        for line in file:
            ip, status = parse_apache_log_line(line.strip())
            if ip and status:
                # Skip private IPs
                if is_private_ip(ip):
                    continue
                # Only count successful requests (status 200)
                if status == '200':
                    ip_counter[ip] += 1

    # Get top N IPs
    top_ips = ip_counter.most_common(top_n)
    return top_ips

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python top_ips.py <apache_access_log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    top_30 = get_top_ips(log_file, top_n=30)

    print("Top 30 IP addresses with status 200 (excluding 10.* and 127.*):")
    for idx, (ip, count) in enumerate(top_30):
        print(f"{idx + 1}\t{ip}\t{count}")