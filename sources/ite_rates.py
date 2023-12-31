link = "https://www.troutdaleoregon.gov/sites/default/files/fileattachments/public_works/page/966/ite_land_use_list_10th_edition.pdf"
headers = """Code; Description;Unit of Measure; Trips Per Unit 1; Trips Per Unit 2;"""
RAW = """
30; Intermodal Truck Terminal; 1,000 SF GFA; 1.72;;
432; Golf Driving Range; 1.25;;
90; Park-and-Ride Lot with Bus Service; Parking Spaces; 0.43;;
433; Batting Cages; Cages; 2.22;;
434; Rock Climbing Gym; 1,000 SF GFA; 1.64;;
110; General Light Industrial; 1,000 SF GFA; 0.63;;
435; Multi-Purpose Recreational Facility; 1,000 SF GFA; 3.58;;
130; Industrial Park; 1,000 SF; GFA 0.40;;
436; Trampoline Park; 1,000 SF; GFA 1.50;;
140; Manufacturing; 1,000 SF GFA; 0.67;;
437; Bowling Alley; 1,000 SF GFA; 1.16;;
150; Warehousing; 1,000 SF GFA; 0.19;;
440; Adult Cabaret; 1,000 SF GFA; 2.93;;
151; Mini-Warehouse; 1,000 SF GFA; 0.17;;
444; Movie Theater; 1,000 SF GFA; 6.17;;
154; High-Cube Transload & Short-Term Storage Warehouse; 1,000 SF GFA; 0.10;;
445; Multiplex Movie Theater; 1,000 SF GFA; 4.91;;
155; High-Cube Fulfillment Center Warehouse; 1,000 SF GFA; 1.37;;
452; Horse Racetrack; Seats; 0.06;;
156; High-Cube Parcel Hub Warehouse; 1,000 SF GFA; 0.64;;
454; Dog Racetrack; Attendees; 0.15;;
157; High-Cube Cold Storage Warehouse; 1,000 SF GFA; 0.12;;
460; Arena; 1,000 SF GFA; 0.47;;
160; Data Center; 1,000 SF GFA; 0.09;;
462; Professional Baseball Stadium; Attendees; 0.15;;
170; Utilities; 1,000 SF GFA; 2.27;;
465; Ice Skating Rink; 1,000 SF GFA; 1.33;;
180; Specialty Trade Contractor; 1,000 SF GFA; 1.97;;
466; Snow Ski Area; Slopes; 26.00;;
473; Casino/Video Lottery Establishment; 1,000 SF GFA; 13.49;;
210; Single-Family Detached Housing; Dwelling Units; 0.99;;
480; Amusement Park; Acres; 3.95;;
220; Multifamily Housing (Low-Rise); Dwelling Units; 0.56;;
482; Water Slide Park; Parking Spaces; 0.28;;
221; Multifamily Housing (Mid-Rise); Dwelling Units; 0.44; 0.18;
488; Soccer Complex; Fields; 16.43;;
222; Multifamily Housing (High-Rise); Dwelling Units; 0.36; 0.19;
490; Tennis Courts; Courts; 4.21;;
231; Mid-Rise Residential with 1st-Floor Commercial; Dwelling Units; 0.36;;
491; Racquet/Tennis Club; Courts; 3.82;;
232; High-Rise Residential with 1st-Floor Commercial; Dwelling Units; 0.21;;
492; Health/Fitness Club; 1,000 SF GFA; 3.45;;
240; Mobile Home Park; Dwelling Units; 0.46;;
493; Athletic Club; 1,000 SF GFA; 6.29;;
251; Senior Adult Housing - Detached; Dwelling Units; 0.30;;
495; Recreational Community Center; 1,000 SF GFA; 2.31;;
252; Senior Adult Housing - Attached; Dwelling Units; 0.26;;
253; Congregate Care Facility; Dwelling Units; 0.18;;
520; Elementary School; 1,000 SF GFA; 1.37;;
254; Assisted Living; 1,000 SF GFA; 0.48;;
522; Middle School / Junior High School; 1,000 SF GFA; 1.19;;
255; Continuing Care Retirement Community; Units; 0.16;;
530; High School; 1,000 SF GFA; 0.97;;
260; Recreation Homes; Dwelling Units; 0.28;;
534; Private School (K-8); Students; 0.26;;
265; Timeshare; Dwelling Units; 0.63;;
536; Private School (K-12); Students; 0.17;;
270; Residential Planned Unit Development; Dwelling Units; 0.69;;
537; Charter Elemantary School; Students; 0.14;;
538; School District Office; 1,000 SF GFA; 2.04;;
310; Hotel; Rooms; 0.60;;
540; Junior / Community College; 1,000 SF GFA; 1.86;;
311; All Suites Hotel; Rooms; 0.36; 0.17;
550; University/College; 1,000 SF GFA; 1.17;;
312; Business Hotel; Rooms; 0.32;;
560; Church; 1,000 SF GFA; 0.49;;
320; Motel; Rooms; 0.38;;
561; Synagogue; 1,000 SF GFA; 2.92;;
330; Resort Hotel; Rooms; 0.41;;
562; Mosque; 1,000 SF GFA; 4.22;;
565; Daycare Center; 1,000 SF GFA; 11.12;;
411; Public Park; Acres; 0.11;;
566; Cemetery; Acres; 0.46;;
416; Campground / Recreation Vehicle Park; Acres; 0.98;;
571; Prison; 1,000 SF GFA; 2.91;;
420; Marina; Berths; 0.21;;
575; Fire and Rescue Station; 1,000 SF GFA; 0.48;;
430; Golf Course; Acres; 0.28;;
580; Museum; 1,000 SF GFA; 0.18;;
431; Miniature Golf Course; Holes; 0.33;;
590; Library; 1,000 SF GFA; 8.16;;
610; Hospital; 1,000 SF GFA; 0.97;;
864; Toy/Children's Superstore; 1,000 SF GFA; 5.00;;
620; Nursing Home; 1,000 SF GFA; 0.59;;
865; Baby Superstore; 1,000 SF GFA; 1.82;;
630; Clinic; 1,000 SF GFA; 3.28; 5.18;
866; Pet Supply Superstore; 1,000 SF GFA; 3.55;;
640; Animal Hospital / Veterinary Clinic; 1,000 SF GFA; 3.53;;
867; Office Supply Superstore; 1,000 SF GFA; 2.77;;
650; Free-Standing Emergency Room; 1,000 SF GFA; 1.52;;
868; Book Superstore; 1,000 SF GFA; 15.83;;
869; Discount Home Furnishing Superstore; 1,000 SF GFA; 1.57;;
710; General Office Building; 1,000 SF GFA; 1.15; 0.87;
872; Bed and Linen Superstore; 1,000 SF GFA; 2.22;;
712; Small Office Building; 1,000 SF GFA; 2.45;;
875; Department Store; 1,000 SF GFA; 1.95;;
714; Corporate Headquarters Building; 1,000 SF GFA; 0.60;;
876; Apparel Store 1,000; SF GFA; 4.12; 1.12;
715; Single Tenant Office Building; 1,000 SF GFA; 1.74;;
879; Arts and Craft Store; 1,000 SF GFA; 6.21;;
720; Medical-Dental Office Building; 1,000 SF GFA; 3.46;;
880; Pharmacy / Drugstore without Drive-Through Window; 1,000 SF GFA; 8.51;;
730; Government Office Building; 1,000 SF GFA; 1.71;;
881; Pharmacy / Drugstore with Drive-Through Window; 1,000 SF GFA; 10.29;;
731; State Motor Vehicles Department; 1,000 SF GFA; 5.20;;
882; Marijuana Dispensary; 1,000 SF GFA; 21.83;;
732; United States Post Office; 1,000 SF GFA; 11.21;;
890; Furniture Store; 1,000 SF GFA; 0.52;;
733; Government Office Complex; 1,000 SF GFA; 2.82;;
897; Medical Equipment Store; 1,000 SF GFA; 1.24;;
750; Office Park; 1,000 SF GFA; 1.07 ;;
899; Liquor Store; 1,000 SF GFA; 16.37;;
760; Research and Development Center; 1,000 SF GFA; 0.49;;
770; Business Park; 1,000 SF GFA; 0.42;;
911; Walk-In Bank; 1,000 SF GFA; 12.13;;
912; Drive-In Bank; 1,000 SF GFA; 20.45;;
810; Tractor Supply Store; 1,000 SF GFA; 1.40;;
918; Hair Salon; 1,000 SF GFA; 1.45;;
811; Construction Equipment Rental Store; 1,000 SF GFA; 0.99;;
920; Copy, Print, and Express Ship Store; 1,000 SF GFA; 7.42;;
812; Building Materials and Lumber Store; 1,000 SF GFA; 2.06;;
925; Drinking Place; 1,000 SF GFA; 11.36;;
813; Free-Standing Discount Superstore; 1,000 SF GFA; 4.33;;
926; Food Cart Pod; Food Carts; 3.08;;
814; Variety Store; 1,000 SF GFA; 6.84;;
930; Fast Casual Restaurant; 1,000 SF GFA; 14.13;;
815; Free Standing Discount Store; 1,000 SF GFA; 4.83;;
931; Quality Restaurant; 1,000 SF GFA; 7.80;;
816; Hardware / Paint Store; 1,000 SF GFA; 2.68;;
932; High-Turnover (Sit-Down) Restaurant; 1,000 SF GFA; 9.77; 9.80;
817; Nursery (Garden Center); 1,000 SF GFA; 6.94;;
933; Fast Food Restaurant without Drive-Through; Window 1,000 SF GFA; 28.34;;
818; Nursery (Wholesale); 1,000 SF GFA; 5.18;;
934; Fast Food Restaurant with Drive-Through Window; 1,000 SF GFA; 32.67; 78.74;
820; Shopping Center; 1,000 SF GFA; 3.81; 4.92;
935; Fast Food Restaurant with Drive-Through Window and No Indoor Seating; 1,000 SF GFA; 42.65;
823; Factory Outlet Center; 1,000 SF GFA; 2.29;;
936; Coffee/Donut Shop without Drive-Through Window; 1,000 SF GFA; 36.31;;
840; Automobile Sales (New); 1,000 SF GFA; 2.43;;
937; Coffee/Donut Shop with Drive-Through Window; 1,000 SF GFA; 43.38; 83.19;
841; Automobile Sales (Used); 1,000 SF GFA; 3.75;;
938; Coffee/Donut Shop with Drive-Through Window and No Indoor Seating; 1,000 SF GFA; 83.33;;
842; Recreational Vehicle Sales; 1,000 SF GFA; 0.77;;
939; Bread / Donut / Bagel Shop without Drive-Through Window; 1,000 SF GFA; 28.00;;
843; Automobile Parts Sales; 1,000 SF GFA; 4.91;;
940; Bread / Donut / Bagel Shop with Drive-Through Window; 1,000 SF GFA; 19.02;;
848; Tire Store; 1,000 SF GFA; 3.98;;
941; Quick Lubrication Vehicle Shop; 1,000 SF GFA; 8.70;;
849; Tire Superstore; 1,000 SF GFA; 2.11;;
942; Automobile Care Center; 1,000 SF GFA; 3.11;;
850; Supermarket; 1,000 SF GFA; 9.24;;
943; Automobile Parts and Service Center; 1,000 SF GFA; 2.26;;
851; Convenience Market (Open 24 Hours); 1,000 SF GFA; 49.11;;
944; Gasoline / Service Station; 1,000 SF GFA; 109.27;;
853; Convenience Market with Gasoline Pumps; 1,000 SF GFA; 49.29;;
945; Gasoline / Service Station with Convenience Market; 1,000 SF GFA; 88.35;;
854; Discount Supermarket; 1,000 SF GFA; 8.38;; 
947; Self Service Car Wash Wash; Stalls; 5.54;;
857; Discount Club; 1,000 SF GFA; 4.18;;
948; Automated Car Wash; 1,000 SF GFA; 14.20;;
860; Wholesale Market; 1,000 SF GFA; 1.76;;
949; Car Wash and Detail Center Wash; Stalls; 13.60;;
861; Sporting Goods Superstore; 1,000 SF GFA; 2.02; 1.65;
950; Truck Stop; 1,000 SF GFA; 22.73;;
862; Home Improvement Superstore 1,000; SF GFA; 2.33; 3.35;
960; Super Convenience Market/Gas Station; 1,000 SF GFA; 69.28;;
863; Electronics Superstore; 1,000 SF GFA; 4.26;;
970; Winery; 1,000 SF GFA; 7.31;;
"""