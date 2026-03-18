import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

CI_LOGO_ON = "CI1002"
CI_LOGO_OFF = "CI1000"

root = ET.Element("Playlist")

now = datetime.now().replace(second=0, microsecond=0)
end = now + timedelta(hours=1)

current = now
segment = 1

logo_on = []
logo_off = []

while current < end:
    # 3 minutes intentional OFF (CI1000)
    off_duration = timedelta(minutes=3)

    multi = ET.SubElement(root, "Event")
    multi.set("Type", "Multi")
    multi.set("Time", current.strftime("%H:%M:%S.00"))
    multi.set("MaterialID", f"SEG{segment}")
    multi.set("Duration", "0:03:00")

    logo = ET.SubElement(root, "Event")
    logo.set("Type", "Logo")
    logo.set("MaterialID", CI_LOGO_OFF)
    logo.set("SOM", "00:00:00.00")
    logo.set("Duration", "0:03:00")

    logo_off.append((current, current + off_duration))

    current += off_duration
    segment += 1

    # 1 minute ON (CI1002)
    on_duration = timedelta(minutes=1)

    multi = ET.SubElement(root, "Event")
    multi.set("Type", "Multi")
    multi.set("Time", current.strftime("%H:%M:%S.00"))
    multi.set("MaterialID", f"SEG{segment}")
    multi.set("Duration", "0:01:00")

    logo = ET.SubElement(root, "Event")
    logo.set("Type", "Logo")
    logo.set("MaterialID", CI_LOGO_ON)
    logo.set("SOM", "00:00:00.00")
    logo.set("Duration", "0:01:00")

    logo_on.append((current, current + on_duration))

    current += on_duration
    segment += 1

tree = ET.ElementTree(root)
tree.write("bbchd.xml", encoding="utf-8", xml_declaration=True)

print("\nLOGO ON WINDOWS (CI1002)")
print("ON FROM   | ON TO")
print("------------------")
for s, e in logo_on:
    print(f"{s.strftime('%H:%M:%S')} | {e.strftime('%H:%M:%S')}")

print("\nLOGO OFF WINDOWS (CI1000)")
print("OFF FROM  | OFF TO")
print("------------------")
for s, e in logo_off:
    print(f"{s.strftime('%H:%M:%S')} | {e.strftime('%H:%M:%S')}")
