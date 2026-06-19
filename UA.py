from device_detector import DeviceDetector

# Single UA
def parse_ua(ua_string):
    device = DeviceDetector(ua_string).parse()
    return {
        "brand": device.device_brand(),
        "model": device.device_model(),
        "os": device.os_name(),
        "os_version": device.os_version(),
        "device_type": device.device_type()
    }

# Test it
ua = "  "
print(parse_ua(ua))