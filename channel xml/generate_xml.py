import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import random

# configuration
SIMULATION_DURATION_MINUTES = 60
SEGMENT_MIN_DURATION = 5
SEGMENT_MAX_DURATION = 12

root = ET.Element("Playlist")

current_time = datetime.now()
end_time = current_time + timedelta(minutes=SIMULATION_DURATION_MINUTES)

event_counter = 1

while current_time < end_time:

    # random segment duration
    seg_minutes = random.randint(SEGMENT_MIN_DURATION, SEGMENT_MAX_DURATION)
    seg_duration = timedelta(minutes=seg_minutes)

    if current_time + seg_duration > end_time:
        seg_duration = end_time - current_time

    duration_str = str(seg_duration)

    # MULTI event (program)
    multi = ET.SubElement(root, "Event")
    multi.set("Type", "Multi")
    multi.set("Number", f"SIM{event_counter:05}")
    multi.set("Date", current_time.strftime("%Y-%m-%dT00:00:00.000Z"))
    multi.set("Time", current_time.strftime("%H:%M:%S.00"))
    multi.set("MaterialID", f"PROGRAM{event_counter}")
    multi.set("Title", "Simulated Program Segment")
    multi.set("Duration", duration_str)
    multi.set("OnAirChannel", "0")
    multi.set("BIN", "255")
    multi.set("Segment", "1")

    # CI logo event (layer 1 logo ON)
    logo = ET.SubElement(root, "Event")
    logo.set("Date", current_time.strftime("%Y-%m-%dT00:00:00.000Z"))
    logo.set("Type", "Logo")
    logo.set("MaterialID", "CI1002")
    logo.set("SOM", "00:00:00.00")
    logo.set("Duration", duration_str)
    logo.set("OnAirChannel", "0")
    logo.set("BIN", "255")
    logo.set("GPI1", "0")
    logo.set("GPI2", "0")

    # optional lower third inside segment
    if seg_minutes > 4:
        mt = ET.SubElement(root, "Event")
        mt.set("Date", current_time.strftime("%Y-%m-%dT00:00:00.000Z"))
        mt.set("Type", "Logo")
        mt.set("MaterialID", "MT1503")
        mt.set("SOM", "00:00:03.00")
        mt.set("Duration", "00:00:02.00")
        mt.set("OnAirChannel", "0")
        mt.set("BIN", "255")
        mt.set("GPI1", "0")
        mt.set("GPI2", "0")
        mt.set("Title", f"SIM{event_counter:04}")

    # optional promo strap
    if seg_minutes > 8:
        tk = ET.SubElement(root, "Event")
        tk.set("Date", current_time.strftime("%Y-%m-%dT00:00:00.000Z"))
        tk.set("Type", "Logo")
        tk.set("MaterialID", "TK1001")
        tk.set("SOM", "00:03:00.00")
        tk.set("Duration", "00:00:30.00")
        tk.set("OnAirChannel", "0")
        tk.set("BIN", "255")
        tk.set("GPI1", "0")
        tk.set("GPI2", "0")
        tk.set("Title", f"PROMO{event_counter}")

    current_time += seg_duration
    event_counter += 1

# write XML
tree = ET.ElementTree(root)
tree.write("simulated_bbc_playlist.xml", encoding="utf-8", xml_declaration=True)

print("Simulation XML generated: simulated_bbc_playlist.xml")