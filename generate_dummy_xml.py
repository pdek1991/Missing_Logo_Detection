import os
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timezone, timedelta

def format_duration(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.00"

def create_dummy_xml(filename="dummy_schedule.xml", prefix="bcehd", on_id="CI1002", off_id="CI0000"):
    dir_name = "channel xml"
    os.makedirs(dir_name, exist_ok=True)
    
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone()

    date_str = now_local.strftime("%Y-%m-%d%z")
    if len(date_str) == 15 and date_str[-5] in ('+', '-'):
        date_str = date_str[:-2] + ":" + date_str[-2:]
    
    time_str = now_local.strftime("%H:%M:%S.00")
    
    root = ET.Element("Playlist")
    
    prog_start = now_local
    prog_time_str = prog_start.strftime("%H:%M:%S.00")
    
    prog_event = ET.SubElement(root, "Event")
    prog_event.set("Type", "Program")
    prog_event.set("Date", date_str)
    prog_event.set("Time", prog_time_str)
    prog_event.set("Duration", "03:00:00.00")
    
    current_time = prog_start
    
    # 4-minute cycles: 3 mins OFF, 1 min ON
    # 3 hours = 180 minutes. 180 / 4 = 45 cycles
    
    schedule_log = []
    
    for i in range(45): 
        # OFF segment (3 mins) - Intentional Break
        off_start_time = current_time
        off_som = current_time - prog_start
        ev = ET.SubElement(root, "Event")
        ev.set("Type", "LOGO")
        ev.set("MaterialID", off_id)
        ev.set("SOM", format_duration(off_som))
        ev.set("Duration", "00:03:00.00")
        current_time += timedelta(minutes=3)
        off_end_time = current_time
        
        # ON segment (1 min)
        on_start_time = current_time
        on_som = current_time - prog_start
        ev = ET.SubElement(root, "Event")
        ev.set("Type", "LOGO")
        ev.set("MaterialID", on_id)
        ev.set("SOM", format_duration(on_som))
        ev.set("Duration", "00:01:00.00")
        current_time += timedelta(minutes=1)
        on_end_time = current_time
        
        schedule_log.append(f"Cycle {i+1:02d}: LOGO OFF [{off_start_time.strftime('%H:%M:%S')} - {off_end_time.strftime('%H:%M:%S')}] | LOGO ON [{on_start_time.strftime('%H:%M:%S')} - {on_end_time.strftime('%H:%M:%S')}]")
    
    xml_str = ET.tostring(root, encoding='utf-8')
    parsed_xml = minidom.parseString(xml_str)
    pretty_xml = parsed_xml.toprettyxml(indent="  ")
    
    timestamp_suffix = now_local.strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(dir_name, f"{prefix}_dummy_{timestamp_suffix}.xml")
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(pretty_xml)
        
    print(f"Dummy XML created: {filepath}")
    print("Schedule Preview for next 3 hours (3m OFF -> 1m ON):")
    for log_entry in schedule_log:
        print(log_entry)

if __name__ == "__main__":
    create_dummy_xml()
