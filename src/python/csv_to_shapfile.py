
#first make layer 
for i in range(1,13):
    arcpy.MakeXYEventLayer_management(r"C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\data\csv\trend_analysis\linear_Thawed_trend_mk_{}.csv".format( str(i)), "longitude", "latitude" , "linear_Thawed_trend_mk_{}_Layer".format( str(i)))

#then we should make the layer to shapefile
for i in range(1,13):
    # for state in ['Transition', 'Frozen', 'Thawed']:
    arcpy.FeatureClassToShapefile_conversion("".format(state, str(i)),
                                                  r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly')
        
#make the shapefile to raster
files = glob.glob('C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly\linear_Thawed_trend_mk_Layer_*.shp')
for file in files:
    file_name = os.path.basename(file)[:-4]
    parts = file_name.split('_')
    name = parts[1] + '_' + parts[-1] + "_all_cells"
    name = file[:-4]
    print(name)
    arcpy.PointToRaster_conversion(file,
                                   "m",
                                   r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_raster\{}'.format(name),
                                   "MEAN",
                                   None,
                                   0.45)
    

#convert grid to tif 
files = glob.glob('C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_raster\thawed_*_all')

for file in files:
    arcpy.RasterToOtherFormat_conversion(file, r"C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_raster", "TIFF")





import arcpy
arcpy.env.workspace = r"C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_raster"
raster_list = arcpy.ListRasters()


# Filter raster list to include only those starting with "thawed" and ending with "all"
filtered_rasters = [raster for raster in raster_list if raster.startswith("thawed") and raster.endswith("all")]

for raster in filtered_list:
...     arcpy.ddd.Reclassify(raster, "VALUE", ranges, r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_raster_reclass\{}'.format(raster[:-4]))
...     print(raster)




ranges = '-2 -0.751 1;-0.751 -0.749 2;-0.749 -0.747 3;-0.747 -0.745 4;-0.745 -0.743 5;-0.743 -0.741 6;-0.741 -0.739 7;-0.739 -0.737 8;-0.737 -0.735 9;-0.735 -0.733 10;-0.733 -0.731 11;-0.731 -0.729 12;-0.729 -0.727 13;-0.727 -0.725 14;-0.725 -0.723 15;-0.723 -0.721 16;-0.721 -0.719 17;-0.719 -0.717 18;-0.717 -0.715 19;-0.715 -0.713 20;-0.713 -0.711 21;-0.711 -0.709 22;-0.709 -0.707 23;-0.707 -0.705 24;-0.705 -0.703 25;-0.703 -0.701 26;-0.701 -0.699 27;-0.699 -0.697 28;-0.697 -0.695 29;-0.695 -0.693 30;-0.693 -0.691 31;-0.691 -0.689 32;-0.689 -0.687 33;-0.687 -0.685 34;-0.685 -0.683 35;-0.683 -0.681 36;-0.681 -0.679 37;-0.679 -0.677 38;-0.677 -0.675 39;-0.675 -0.673 40;-0.673 -0.671 41;-0.671 -0.669 42;-0.669 -0.667 43;-0.667 -0.665 44;-0.665 -0.663 45;-0.663 -0.661 46;-0.661 -0.659 47;-0.659 -0.657 48;-0.657 -0.655 49;-0.655 -0.653 50;-0.653 -0.651 51;-0.651 -0.649 52;-0.649 -0.647 53;-0.647 -0.645 54;-0.645 -0.643 55;-0.643 -0.641 56;-0.641 -0.639 57;-0.639 -0.637 58;-0.637 -0.635 59;-0.635 -0.633 60;-0.633 -0.631 61;-0.631 -0.629 62;-0.629 -0.627 63;-0.627 -0.625 64;-0.625 -0.623 65;-0.623 -0.621 66;-0.621 -0.619 67;-0.619 -0.617 68;-0.617 -0.615 69;-0.615 -0.613 70;-0.613 -0.611 71;-0.611 -0.609 72;-0.609 -0.607 73;-0.607 -0.605 74;-0.605 -0.603 75;-0.603 -0.601 76;-0.601 -0.599 77;-0.599 -0.597 78;-0.597 -0.595 79;-0.595 -0.593 80;-0.593 -0.591 81;-0.591 -0.589 82;-0.589 -0.587 83;-0.587 -0.585 84;-0.585 -0.583 85;-0.583 -0.581 86;-0.581 -0.579 87;-0.579 -0.577 88;-0.577 -0.575 89;-0.575 -0.573 90;-0.573 -0.571 91;-0.571 -0.569 92;-0.569 -0.567 93;-0.567 -0.565 94;-0.565 -0.563 95;-0.563 -0.561 96;-0.561 -0.559 97;-0.559 -0.557 98;-0.557 -0.555 99;-0.555 -0.553 100;-0.553 -0.551 101;-0.551 -0.549 102;-0.549 -0.547 103;-0.547 -0.545 104;-0.545 -0.543 105;-0.543 -0.541 106;-0.541 -0.539 107;-0.539 -0.537 108;-0.537 -0.535 109;-0.535 -0.533 110;-0.533 -0.531 111;-0.531 -0.529 112;-0.529 -0.527 113;-0.527 -0.525 114;-0.525 -0.523 115;-0.523 -0.521 116;-0.521 -0.519 117;-0.519 -0.517 118;-0.517 -0.515 119;-0.515 -0.513 120;-0.513 -0.511 121;-0.511 -0.509 122;-0.509 -0.507 123;-0.507 -0.505 124;-0.505 -0.503 125;-0.503 -0.501 126;-0.501 -0.499 127;-0.499 -0.497 128;-0.497 -0.495 129;-0.495 -0.493 130;-0.493 -0.491 131;-0.491 -0.489 132;-0.489 -0.487 133;-0.487 -0.485 134;-0.485 -0.483 135;-0.483 -0.481 136;-0.481 -0.479 137;-0.479 -0.477 138;-0.477 -0.475 139;-0.475 -0.473 140;-0.473 -0.471 141;-0.471 -0.469 142;-0.469 -0.467 143;-0.467 -0.465 144;-0.465 -0.463 145;-0.463 -0.461 146;-0.461 -0.459 147;-0.459 -0.457 148;-0.457 -0.455 149;-0.455 -0.453 150;-0.453 -0.451 151;-0.451 -0.449 152;-0.449 -0.447 153;-0.447 -0.445 154;-0.445 -0.443 155;-0.443 -0.441 156;-0.441 -0.439 157;-0.439 -0.437 158;-0.437 -0.435 159;-0.435 -0.433 160;-0.433 -0.431 161;-0.431 -0.429 162;-0.429 -0.427 163;-0.427 -0.425 164;-0.425 -0.423 165;-0.423 -0.421 166;-0.421 -0.419 167;-0.419 -0.417 168;-0.417 -0.415 169;-0.415 -0.413 170;-0.413 -0.411 171;-0.411 -0.409 172;-0.409 -0.407 173;-0.407 -0.405 174;-0.405 -0.403 175;-0.403 -0.401 176;-0.401 -0.399 177;-0.399 -0.397 178;-0.397 -0.395 179;-0.395 -0.393 180;-0.393 -0.391 181;-0.391 -0.389 182;-0.389 -0.387 183;-0.387 -0.385 184;-0.385 -0.383 185;-0.383 -0.381 186;-0.381 -0.379 187;-0.379 -0.377 188;-0.377 -0.375 189;-0.375 -0.373 190;-0.373 -0.371 191;-0.371 -0.369 192;-0.369 -0.367 193;-0.367 -0.365 194;-0.365 -0.363 195;-0.363 -0.361 196;-0.361 -0.359 197;-0.359 -0.357 198;-0.357 -0.355 199;-0.355 -0.353 200;-0.353 -0.351 201;-0.351 -0.349 202;-0.349 -0.347 203;-0.347 -0.345 204;-0.345 -0.343 205;-0.343 -0.341 206;-0.341 -0.339 207;-0.339 -0.337 208;-0.337 -0.335 209;-0.335 -0.333 210;-0.333 -0.331 211;-0.331 -0.329 212;-0.329 -0.327 213;-0.327 -0.325 214;-0.325 -0.323 215;-0.323 -0.321 216;-0.321 -0.319 217;-0.319 -0.317 218;-0.317 -0.315 219;-0.315 -0.313 220;-0.313 -0.311 221;-0.311 -0.309 222;-0.309 -0.307 223;-0.307 -0.305 224;-0.305 -0.303 225;-0.303 -0.301 226;-0.301 -0.299 227;-0.299 -0.297 228;-0.297 -0.295 229;-0.295 -0.293 230;-0.293 -0.291 231;-0.291 -0.289 232;-0.289 -0.287 233;-0.287 -0.285 234;-0.285 -0.283 235;-0.283 -0.281 236;-0.281 -0.279 237;-0.279 -0.277 238;-0.277 -0.275 239;-0.275 -0.273 240;-0.273 -0.271 241;-0.271 -0.269 242;-0.269 -0.267 243;-0.267 -0.265 244;-0.265 -0.263 245;-0.263 -0.261 246;-0.261 -0.259 247;-0.259 -0.257 248;-0.257 -0.255 249;-0.255 -0.253 250;-0.253 -0.251 251;-0.251 -0.249 252;-0.249 -0.247 253;-0.247 -0.245 254;-0.245 -0.243 255;-0.243 -0.241 256;-0.241 -0.239 257;-0.239 -0.237 258;-0.237 -0.235 259;-0.235 -0.233 260;-0.233 -0.231 261;-0.231 -0.229 262;-0.229 -0.227 263;-0.227 -0.225 264;-0.225 -0.223 265;-0.223 -0.221 266;-0.221 -0.219 267;-0.219 -0.217 268;-0.217 -0.215 269;-0.215 -0.213 270;-0.213 -0.211 271;-0.211 -0.209 272;-0.209 -0.207 273;-0.207 -0.205 274;-0.205 -0.203 275;-0.203 -0.201 276;-0.201 -0.199 277;-0.199 -0.197 278;-0.197 -0.195 279;-0.195 -0.193 280;-0.193 -0.191 281;-0.191 -0.189 282;-0.189 -0.187 283;-0.187 -0.185 284;-0.185 -0.183 285;-0.183 -0.181 286;-0.181 -0.179 287;-0.179 -0.177 288;-0.177 -0.175 289;-0.175 -0.173 290;-0.173 -0.171 291;-0.171 -0.169 292;-0.169 -0.167 293;-0.167 -0.165 294;-0.165 -0.163 295;-0.163 -0.161 296;-0.161 -0.159 297;-0.159 -0.157 298;-0.157 -0.155 299;-0.155 -0.153 300;-0.153 -0.151 301;-0.151 -0.149 302;-0.149 -0.147 303;-0.147 -0.145 304;-0.145 -0.143 305;-0.143 -0.141 306;-0.141 -0.139 307;-0.139 -0.137 308;-0.137 -0.135 309;-0.135 -0.133 310;-0.133 -0.131 311;-0.131 -0.129 312;-0.129 -0.127 313;-0.127 -0.125 314;-0.125 -0.123 315;-0.123 -0.121 316;-0.121 -0.119 317;-0.119 -0.117 318;-0.117 -0.115 319;-0.115 -0.113 320;-0.113 -0.111 321;-0.111 -0.109 322;-0.109 -0.107 323;-0.107 -0.105 324;-0.105 -0.103 325;-0.103 -0.101 326;-0.101 -0.099 327;-0.099 -0.097 328;-0.097 -0.095 329;-0.095 -0.093 330;-0.093 -0.091 331;-0.091 -0.089 332;-0.089 -0.087 333;-0.087 -0.085 334;-0.085 -0.083 335;-0.083 -0.081 336;-0.081 -0.079 337;-0.079 -0.077 338;-0.077 -0.075 339;-0.075 -0.073 340;-0.073 -0.071 341;-0.071 -0.069 342;-0.069 -0.067 343;-0.067 -0.065 344;-0.065 -0.063 345;-0.063 -0.061 346;-0.061 -0.059 347;-0.059 -0.057 348;-0.057 -0.055 349;-0.055 -0.053 350;-0.053 -0.051 351;-0.051 -0.049 352;-0.049 -0.047 353;-0.047 -0.045 354;-0.045 -0.043 355;-0.043 -0.041 356;-0.041 -0.039 357;-0.039 -0.037 358;-0.037 -0.035 359;-0.035 -0.033 360;-0.033 -0.031 361;-0.031 -0.029 362;-0.029 -0.027 363;-0.027 -0.025 364;-0.025 -0.023 365;-0.023 -0.021 366;-0.021 -0.019 367;-0.019 -0.017 368;-0.017 -0.015 369;-0.015 -0.013 370;-0.013 -0.011 371;-0.011 -0.009 372;-0.009 -0.007 373;-0.007 -0.005 374;-0.005 -0.003 375;-0.003 -0.001 376;-0.001 0.001 377;0.001 0.003 378;0.003 0.005 379;0.005 0.007 380;0.007 0.009 381;0.009 0.011 382;0.011 0.013 383;0.013 0.015 384;0.015 0.017 385;0.017 0.019 386;0.019 0.021 387;0.021 0.023 388;0.023 0.025 389;0.025 0.027 390;0.027 0.029 391;0.029 0.031 392;0.031 0.033 393;0.033 0.035 394;0.035 0.037 395;0.037 0.039 396;0.039 0.041 397;0.041 0.043 398;0.043 0.045 399;0.045 0.047 400;0.047 0.049 401;0.049 0.051 402;0.051 0.053 403;0.053 0.055 404;0.055 0.057 405;0.057 0.059 406;0.059 0.061 407;0.061 0.063 408;0.063 0.065 409;0.065 0.067 410;0.067 0.069 411;0.069 0.071 412;0.071 0.073 413;0.073 0.075 414;0.075 0.077 415;0.077 0.079 416;0.079 0.081 417;0.081 0.083 418;0.083 0.085 419;0.085 0.087 420;0.087 0.089 421;0.089 0.091 422;0.091 0.093 423;0.093 0.095 424;0.095 0.097 425;0.097 0.099 426;0.099 0.101 427;0.101 0.103 428;0.103 0.105 429;0.105 0.107 430;0.107 0.109 431;0.109 0.111 432;0.111 0.113 433;0.113 0.115 434;0.115 0.117 435;0.117 0.119 436;0.119 0.121 437;0.121 0.123 438;0.123 0.125 439;0.125 0.127 440;0.127 0.129 441;0.129 0.131 442;0.131 0.133 443;0.133 0.135 444;0.135 0.137 445;0.137 0.139 446;0.139 0.141 447;0.141 0.143 448;0.143 0.145 449;0.145 0.147 450;0.147 0.149 451;0.149 0.151 452;0.151 0.153 453;0.153 0.155 454;0.155 0.157 455;0.157 0.159 456;0.159 0.161 457;0.161 0.163 458;0.163 0.165 459;0.165 0.167 460;0.167 0.169 461;0.169 0.171 462;0.171 0.173 463;0.173 0.175 464;0.175 0.177 465;0.177 0.179 466;0.179 0.181 467;0.181 0.183 468;0.183 0.185 469;0.185 0.187 470;0.187 0.189 471;0.189 0.191 472;0.191 0.193 473;0.193 0.195 474;0.195 0.197 475;0.197 0.199 476;0.199 0.201 477;0.201 0.203 478;0.203 0.205 479;0.205 0.207 480;0.207 0.209 481;0.209 0.211 482;0.211 0.213 483;0.213 0.215 484;0.215 0.217 485;0.217 0.219 486;0.219 0.221 487;0.221 0.223 488;0.223 0.225 489;0.225 0.227 490;0.227 0.229 491;0.229 0.231 492;0.231 0.233 493;0.233 0.235 494;0.235 0.237 495;0.237 0.239 496;0.239 0.241 497;0.241 0.243 498;0.243 0.245 499;0.245 0.247 500;0.247 0.249 501;0.249 0.251 502;0.251 0.253 503;0.253 0.255 504;0.255 0.257 505;0.257 0.259 506;0.259 0.261 507;0.261 0.263 508;0.263 0.265 509;0.265 0.267 510;0.267 0.269 511;0.269 0.271 512;0.271 0.273 513;0.273 0.275 514;0.275 0.277 515;0.277 0.279 516;0.279 0.281 517;0.281 0.283 518;0.283 0.285 519;0.285 0.287 520;0.287 0.289 521;0.289 0.291 522;0.291 0.293 523;0.293 0.295 524;0.295 0.297 525;0.297 0.299 526;0.299 0.301 527;0.301 0.303 528;0.303 0.305 529;0.305 0.307 530;0.307 0.309 531;0.309 0.311 532;0.311 0.313 533;0.313 0.315 534;0.315 0.317 535;0.317 0.319 536;0.319 0.321 537;0.321 0.323 538;0.323 0.325 539;0.325 0.327 540;0.327 0.329 541;0.329 0.331 542;0.331 0.333 543;0.333 0.335 544;0.335 0.337 545;0.337 0.339 546;0.339 0.341 547;0.341 0.343 548;0.343 0.345 549;0.345 0.347 550;0.347 0.349 551;0.349 0.351 552;0.351 0.353 553;0.353 0.355 554;0.355 0.357 555;0.357 0.359 556;0.359 0.361 557;0.361 0.363 558;0.363 0.365 559;0.365 0.367 560;0.367 0.369 561;0.369 0.371 562;0.371 0.373 563;0.373 0.375 564;0.375 0.377 565;0.377 0.379 566;0.379 0.381 567;0.381 0.383 568;0.383 0.385 569;0.385 0.387 570;0.387 0.389 571;0.389 0.391 572;0.391 0.393 573;0.393 0.395 574;0.395 0.397 575;0.397 0.399 576;0.399 0.401 577;0.401 0.403 578;0.403 0.405 579;0.405 0.407 580;0.407 0.409 581;0.409 0.411 582;0.411 0.413 583;0.413 0.415 584;0.415 0.417 585;0.417 0.419 586;0.419 0.421 587;0.421 0.423 588;0.423 0.425 589;0.425 0.427 590;0.427 0.429 591;0.429 0.431 592;0.431 0.433 593;0.433 0.435 594;0.435 0.437 595;0.437 0.439 596;0.439 0.441 597;0.441 0.443 598;0.443 0.445 599;0.445 0.447 600;0.447 0.449 601;0.449 0.451 602;0.451 0.453 603;0.453 0.455 604;0.455 0.457 605;0.457 0.459 606;0.459 0.461 607;0.461 0.463 608;0.463 0.465 609;0.465 0.467 610;0.467 0.469 611;0.469 0.471 612;0.471 0.473 613;0.473 0.475 614;0.475 0.477 615;0.477 0.479 616;0.479 0.481 617;0.481 0.483 618;0.483 0.485 619;0.485 0.487 620;0.487 0.489 621;0.489 0.491 622;0.491 0.493 623;0.493 0.495 624;0.495 0.497 625;0.497 0.499 626;0.499 0.501 627;0.501 0.503 628;0.503 0.505 629;0.505 0.507 630;0.507 0.509 631;0.509 0.511 632;0.511 0.513 633;0.513 0.515 634;0.515 0.517 635;0.517 0.519 636;0.519 0.521 637;0.521 0.523 638;0.523 0.525 639;0.525 0.527 640;0.527 0.529 641;0.529 0.531 642;0.531 0.533 643;0.533 0.535 644;0.535 0.537 645;0.537 0.539 646;0.539 0.541 647;0.541 0.543 648;0.543 0.545 649;0.545 0.547 650;0.547 0.549 651;0.549 0.551 652;0.551 0.553 653;0.553 0.555 654;0.555 0.557 655;0.557 0.559 656;0.559 0.561 657;0.561 0.563 658;0.563 0.565 659;0.565 0.567 660;0.567 0.569 661;0.569 0.571 662;0.571 0.573 663;0.573 0.575 664;0.575 0.577 665;0.577 0.579 666;0.579 0.581 667;0.581 0.583 668;0.583 0.585 669;0.585 0.587 670;0.587 0.589 671;0.589 0.591 672;0.591 0.593 673;0.593 0.595 674;0.595 0.597 675;0.597 0.599 676;0.599 0.601 677;0.601 0.603 678;0.603 0.605 679;0.605 0.607 680;0.607 0.609 681;0.609 0.611 682;0.611 0.613 683;0.613 0.615 684;0.615 0.617 685;0.617 0.619 686;0.619 0.621 687;0.621 0.623 688;0.623 0.625 689;0.625 0.627 690;0.627 0.629 691;0.629 0.631 692;0.631 0.633 693;0.633 0.635 694;0.635 0.637 695;0.637 0.639 696;0.639 0.641 697;0.641 0.643 698;0.643 0.645 699;0.645 0.647 700;0.647 0.649 701;0.649 0.651 702;0.651 0.653 703;0.653 0.655 704;0.655 0.657 705;0.657 0.659 706;0.659 0.661 707;0.661 0.663 708;0.663 0.665 709;0.665 0.667 710;0.667 0.669 711;0.669 0.671 712;0.671 0.673 713;0.673 0.675 714;0.675 0.677 715;0.677 0.679 716;0.679 0.681 717;0.681 0.683 718;0.683 0.685 719;0.685 0.687 720;0.687 0.689 721;0.689 0.691 722;0.691 0.693 723;0.693 0.695 724;0.695 0.697 725;0.697 0.699 726;0.699 0.701 727;0.701 0.703 728;0.703 0.705 729;0.705 0.707 730;0.707 0.709 731;0.709 0.711 732;0.711 0.713 733;0.713 0.715 734;0.715 0.717 735;0.717 0.719 736;0.719 0.721 737;0.721 0.723 738;0.723 0.725 739;0.725 0.727 740;0.727 0.729 741;0.729 0.731 742;0.731 0.733 743;0.733 0.735 744;0.735 0.737 745;0.737 0.739 746;0.739 0.741 747;0.741 0.743 748;0.743 0.745 749;0.745 0.747 750;0.747 0.749 751;0.749 0.751 752;;0.751 2 754'