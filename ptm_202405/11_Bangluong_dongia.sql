create table ttkd_bsc.tonghop_dtdongia_ptm_202404_l1 as select * from ttkd_bsc.tonghop_dtdongia_ptm_202404;
create table ttkd_bsc.bangluong_dongia_202404_l5 as select * from ttkd_bsc.bangluong_dongia_202404; 
create table ttkd_bsc.bangluong_dongia_202405_l4 as select * from ttkd_bsc.bangluong_dongia_202405; 
rename bangluong_dongia_202405 to bangluong_dongia_202405_new;
rename bangluong_dongia_202405_l4 to bangluong_dongia_202405;
select * from ttkd_bsc.bangluong_dongia_202404_l4_bs_202405_2nv;

---lan dau tien chay doan Frame nay, lan sau khong chay doan FRAME nay
------BEGIN dot 1---khong chay cho dot 2
drop table ttkd_bsc.bangluong_dongia_202405;
				create table ttkd_bsc.bangluong_dongia_202405 (
				ma_nv               varchar(10),
				ten_nv               varchar(50),
				ma_vtcv            varchar(20),
				ten_vtcv            varchar(100),
				ma_pb         varchar(10),
				ten_pb         varchar(50),
				ma_to               varchar(10),
				ten_to               varchar(100),
				loai_ld               varchar(50),
--				thang_hd          number,
				dtptm_dongia_cdbr        number,
				dtptm_dongia_cntt         number,
				dtptm_dongia_cntt_qd   number,
				dtptm_dongia_vnpts      number,
				dtptm_dongia_vnpts_tong   number,
				tong_dtptm                     number, 
				dtptm_quydinh               number,
				dtptm_muctieu               number,
				dtptm_bq3t                     number,
				heso_qd_tong                 number(5,2),
				
				luong_dongia_cdbr        number,
				luong_dongia_cntt         number,
				luong_dongia_vnpts      number,
				
				ctvxhh_qly_ptr_ctv          number,
				luong_dongia_dnhm_vnptt   number, 
				luong_dongia_goi_kpbdb   number, 
				luong_dongia_goi_hcm   number, 
				luong_dongia_goi_qldb   number,
				luong_dongia_vnptt         number,
				
				luong_dongia_ghtt           number,
				ghtt_vnpts                         number,
				luong_khac                       number,
				tong_luong_dongia         number,
				
				ghichu                               varchar2(500),
				luong_dongia_ptm_thuhoi number,
				thuhoi_dongia_ghtt number,
				giamtru_hosotainha               number,
				giamtru_phathuy_qldb          number,
				giamtru_ghtt_cntt				number,
				luong_dongia_khac_thuhoi    number,
				tong_luong_thuhoi                number
				)
	;


			insert into ttkd_bsc.bangluong_dongia_202405
				 (ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld)
			SELECT ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld
			  FROM ttkd_bsc.nhanvien
			  where donvi = 'TTKD' and thang = 202405
			  ; 


-- Doanh thu dinh muc - 268 /TTr- NS 05/09/2022
-- Ins tam danh muc dinh muc dthu tu ky truoc de tinh cho dot 1:  
			insert into ttkd_bsc.dinhmuc_dthu_ptm 
							(thang, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ten_to, ma_pb, ten_pb, muc_quy_dinh, dt_giao_bsc, dt_didong_giao, ghi_chu, DTPTM_MUCTIEU) 
			    
			    select 202405, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ten_to, ma_pb, ten_pb,muc_quy_dinh, dt_giao_bsc, dt_didong_giao, ghi_chu, DTPTM_MUCTIEU
			    from ttkd_bsc.dinhmuc_dthu_ptm 
			    where thang = 202404		---thang n-1 vi Vinh chua gui file, nen tam sd file cu
--								and ma_nv in ('CTV084000',
--															'CTV084314',
--															'CTV085604',
--															'CTV085605',
--															'CTV085665',
--															'CTV085345')
		;


-- Xoa, imp file moi sau khi Vinh gui file chinh thuc de tinh cho dot 2:
			delete from ttkd_bsc.dinhmuc_dthu_ptm 
			    where thang=202405
			;
       commit;
			update ttkd_bsc.dinhmuc_dthu_ptm set thang=202404
			    where thang is null;
     

-- dtptm_quydinh, dtptm_muctieu:
			alter table ttkd_bsc.bangluong_dongia_202405 add dtptm_quydinh_g number
			;
			update ttkd_bsc.bangluong_dongia_202405 a set dtptm_quydinh ='', dtptm_muctieu=''
			;
			select* from ttkd_bsc.dinhmuc_dthu_ptm 
			where thang=202405 and dtptm_muctieu is null
			;
			rollback;
			commit;
--			alter table ttkd_bsc.dinhmuc_dthu_ptm add dtptm_muctieu number
			;
			update ttkd_bsc.dinhmuc_dthu_ptm a
					  set dtptm_muctieu= 
							    (case muc_quy_dinh 
								   when 52500000 then 68000000
								   when 35000000 then 70200000 -- Nhân viên qu?n lý ?i?m bán: ap theo VB la 70200000, da hoi Xuan Vinh PNS 05/05/2023
								   when 31500000 then 40800000
								   when 16000000 then 20800000
								   when 15700000 then 20500000
								   when 14500000 then 18890000
								   when 13000000 then 16900000
								   when 7800000 then 10100000
								   when 7300000 then 16900000  -- T? Qu?n Lý ??a Bàn cua BHCC, T? Qu?n lý ??a bàn C?n Gi? cua NSG van ap theo VB la 16900000, da hoi Xuan Vinh PNS 05/05/2023
								   when 5100000 then 6600000
								   when 2600000 then 3400000
							    else null end)
				   where muc_quy_dinh>0 and thang = 202405-- and ma_nv in ('CTV084000','CTV084314','CTV085604','CTV085605','CTV085665','CTV085345');
	   ;
			update ttkd_bsc.dinhmuc_dthu_ptm a 
						set dtptm_muctieu = (select distinct dtptm_muctieu 
																	from ttkd_bsc.dinhmuc_dthu_ptm 
																	where dtptm_muctieu > 0 and thang = 202405 and ma_pb = a.ma_pb and ma_vtcv = a.ma_vtcv)
		--	select * from ttkd_bsc.dinhmuc_dthu_ptm a
						where dtptm_muctieu is null and thang = 202405
			;
--			update ttkd_bsc.dinhmuc_dthu_ptm a 
--						set dtptm_muctieu = 40800000
--			where dtptm_muctieu is null and thang = 202405 and ma_vtcv = 'VNP-HNHCM_KHDN_18'
			;
			
			update ttkd_bsc.bangluong_dongia_202405 a
					  set (dtptm_quydinh_g, dtptm_quydinh, dtptm_muctieu)=(select muc_quy_dinh, dt_giao_bsc, dtptm_muctieu from ttkd_bsc.dinhmuc_dthu_ptm where thang=202405 and ma_nv=a.ma_nv)
			    -- select ma_nv, ten_vtcv, ten_to, ten_pb, dtptm_quydinh_g, dtptm_quydinh, dtptm_muctieu, (select dt_giao_bsc from ttkd_bsc.dinhmuc_dthu_ptm where thang=202405 and ma_nv=a.ma_nv) dieuchinh_dthu_quydinh from ttkd_bsc.bangluong_dongia_202405 a
			    where exists(select dt_giao_bsc from ttkd_bsc.dinhmuc_dthu_ptm where thang=202405 and ma_nv=a.ma_nv)
			    ;

-- khong chay cho dot 2----Doanh thu binh quan 3 thang truoc lien ke: khong bao gom VNPtt 
			drop table ttkd_bsc.temp_dtbq purge
			;
			select * from ttkd_bsc.temp_dtbq;
			---thang n-1, n-2, n-3
				create table ttkd_bsc.temp_dtbq as
							select ma_nv, sum(tong_dtptm) tong_dtptm, round( sum(tong_dtptm)/3,0) dtbq
							from (select 202404 thang, ma_nv, DTPTM_DONGIA_CDBR, DTPTM_DONGIA_CNTT DTPTM_DONGIA_CNTT, DTPTM_DONGIA_VNPTS,
													nvl(DTPTM_DONGIA_CDBR ,0) + nvl(DTPTM_DONGIA_CNTT ,0) + nvl(DTPTM_DONGIA_VNPTS ,0)  tong_dtptm
										    from ttkd_bsc.bangluong_dongia_202404
										   
										   union all
													   select 202403 thang, ma_nv, dtptm_dongia_cdbr, dtptm_dongia_cntt dtptm_dongia_cntt, dtptm_dongia_vnpts,
																nvl(dtptm_dongia_cdbr ,0) + nvl(dtptm_dongia_cntt ,0) + nvl(dtptm_dongia_vnpts ,0)  tong_dtptm
													    from ttkd_bsc.bangluong_dongia_202403
										   
										   union all
														   select 202402 thang, ma_nv, dtptm_dongia_cdbr, dtptm_dongia_cntt dtptm_dongia_cntt, dtptm_dongia_vnpts,
																	nvl(dtptm_dongia_cdbr ,0) + nvl(dtptm_dongia_cntt ,0) + nvl(dtptm_dongia_vnpts ,0)  tong_dtptm
														   from ttkd_bsc.bangluong_dongia_202402
								   ) group by ma_nv
						;

			update ttkd_bsc.bangluong_dongia_202405 a
	
				set dtptm_bq3t='' 
		---  select * from ttkd_bsc.bangluong_dongia_202405 a
				where dtptm_bq3t is not null
				;
 
     
			update ttkd_bsc.bangluong_dongia_202405 a
				   set dtptm_bq3t =(select dtbq from ttkd_bsc.temp_dtbq where ma_nv=a.ma_nv)
		---select dtptm_bq3t from ttkd_bsc.bangluong_dongia_202405 a
			    where exists (select dtbq from ttkd_bsc.temp_dtbq where ma_nv=a.ma_nv)
			    ;

commit;
rollback;

------END dot 1---khong chay cho dot 2
create table  ttkd_bsc.tonghop_dtdongia_ptm_202405_l2 as select * from ttkd_bsc.tonghop_dtdongia_ptm_202405;
	
		---tao bang tong hop
drop table ttkd_bsc.tonghop_dtdongia_ptm_202405 purge
;
select sum(luong_ptm_cdbr + luong_ptm_vnpts + luong_ptm_cntt)  from ttkd_bsc.tonghop_dtdongia_ptm_202405 where ma_nv in (select ma_nv from ttkd_bsc.bangluong_dongia_202405)
;
select sum(nvl(LUONG_DONGIA_CDBR, 0)+ nvl(LUONG_DONGIA_CNTT, 0)+ nvl(LUONG_DONGIA_VNPTS, 0))  from ttkd_bsc.bangluong_dongia_202405_l2
;
		create table ttkd_bsc.tonghop_dtdongia_ptm_202405 as
					select manv_ptm ma_nv, 
							  ma_to,
							  ma_pb,
							  sum(nvl(dthu_ptm_cdbr,0)) dthu_ptm_cdbr, 
							  sum(nvl(dthu_ptm_vnpts,0)) dthu_ptm_vnpts,
							  0 dthu_ptm_vnptt,
							  sum(nvl(dthu_ptm_khac,0)) dthu_ptm_cntt, 
							  round(sum(nvl(luong_ptm_cdbr,0)) ,0)  luong_ptm_cdbr, 
							  round(sum(nvl(luong_ptm_vnpts,0)) ,0) luong_ptm_vnpts,
							  0 luong_ptm_vnptt,
							  round(sum(nvl(luong_ptm_khac,0)) ,0) luong_ptm_cntt
					from
					(
					    -- nvptm
					    select  manv_ptm,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvptm end) dthu_ptm_cdbr,
							  sum(case when loaitb_id=20 then doanhthu_dongia_nvptm end) dthu_ptm_vnpts,
							  sum(case when loaitb_id=21  then doanhthu_dongia_nvptm end) dthu_ptm_vnptt,
							  sum(case when dichvuvt_id in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvptm end) dthu_ptm_khac,
							  sum(doanhthu_dongia_nvptm) Tong_dthu,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvptm end) luong_ptm_cdbr,
							  sum(case when loaitb_id=20  then luong_dongia_nvptm end) luong_ptm_vnpts,
							  0 luong_ptm_vnptt,
							  sum(case when dichvuvt_id in (13,14,15,16) or loaitb_id is null then luong_dongia_nvptm end) luong_ptm_khac
							  ,'nvptm' nguon, thang_tldg_dt thang
					    from ttkd_bsc.ct_bsc_ptm
					    where thang_tldg_dt = 202405
					    group by manv_ptm, thang_tldg_dt
					    
		union all     
					    -- dnhm cua nvptm
					    select  manv_ptm,
									  sum(case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dnhm end) dthu_ptm_cdbr,
									  sum(case when loaitb_id=20 then doanhthu_dongia_dnhm end) dthu_ptm_vnpts, 
									  null dthu_ptm_vnptt, 
									  sum(case when loaitb_id in (38,127) then doanhthu_dongia_dnhm end)  dthu_ptm_khac,
									  sum(doanhthu_dongia_dnhm) Tong_dthu,
									  sum(case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dnhm_nvptm end) luong_ptm_cdbr,
									  sum(case when loaitb_id=20 then luong_dongia_dnhm_nvptm end) luong_ptm_vnpts,
									  null luong_ptm_vnptt, 
									  sum(case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm end) luong_ptm_khac
									 ,'dnhm' nguon, thang_tldg_dnhm
					    from ttkd_bsc.ct_bsc_ptm
					    where thang_tldg_dnhm = 202405
					    group by manv_ptm, thang_tldg_dnhm
					    
		union all     
					    -- nv dai
					    select  manv_tt_dai,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dai end) dthu_ptm_cdbr,
							  sum(case when loaitb_id=20  then doanhthu_dongia_dai end) dthu_ptm_vnpts,
							  sum(case when loaitb_id=21  then doanhthu_dongia_dai end) dthu_ptm_vnptt,
							  sum(case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_dai end) dthu_ptm_khac,
							  sum(doanhthu_dongia_dai) Tong_dthu,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dai end) luong_ptm_cdbr,
							  sum(case when loaitb_id=20  then luong_dongia_dai end) luong_ptm_vnpts,
							 0 luong_ptm_vnptt,
							  sum(case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_dai end) luong_ptm_khac     
							,'nvtt dai' nguon, thang_tldg_dt_dai
					    from ttkd_bsc.ct_bsc_ptm
					    where thang_tldg_dt_dai = 202405
					    group by manv_tt_dai, thang_tldg_dt_dai
		
		
		union all     
					    -- nv gioi thieu DIGI, SHOP, ngoai tru  phong GP
					    select  manv_hotro,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvhotro end) dthu_ptm_cdbr,
							  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dthu_ptm_vnpts,
							  sum(case when loaitb_id=21  then doanhthu_dongia_nvhotro end) dthu_ptm_vnptt,
							  sum(case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvhotro end) dthu_ptm_khac,
							  sum(doanhthu_dongia_nvhotro) Tong_dthu,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvhotro end) luong_ptm_cdbr,
							  sum(case when loaitb_id=20  then luong_dongia_nvhotro end) luong_ptm_vnpts,
							 0 luong_ptm_vnptt,
							  sum(case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_nvhotro end) luong_ptm_khac     
							,'nvtt gioithieu' nguon, thang_tldg_dt_nvhotro
					    from ttkd_bsc.ct_bsc_ptm a
					    where thang_tldg_dt_nvhotro = 202405
									and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202405 and ma_pb='VNP0702600' and ma_nv = a.manv_hotro)
					    group by manv_hotro, thang_tldg_dt_nvhotro
					
					   
		union all     
					    -- nv ho tro cua PGP
					    select  manv_hotro,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvhotro end) dthu_ptm_cdbr,
							  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dthu_ptm_vnpts,
							  sum(case when loaitb_id=21  then doanhthu_dongia_nvhotro end) dthu_ptm_vnptt,
							  sum(case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvhotro end) dthu_ptm_khac,
							  sum(doanhthu_dongia_nvhotro) Tong_dthu,
							  sum(case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvhotro end) luong_ptm_cdbr,
							  sum(case when loaitb_id=20  then luong_dongia_nvhotro end) luong_ptm_vnpts,
							 0 luong_ptm_vnptt,
							  sum(case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_nvhotro end) luong_ptm_khac     
							,'manv_hotro' nguon, thang_tldg_dt_nvhotro
					    from ttkd_bsc.ct_bsc_ptm_pgp
					    where thang_tldg_dt_nvhotro = 202405 
					    group by manv_hotro, thang_tldg_dt_nvhotro
			    ) a
			    
			left join ttkd_bsc.nhanvien nv on nv.thang = a.thang and nv.ma_nv = a.manv_ptm
			where manv_ptm is not null
			group by manv_ptm, ma_to, ma_pb
			; 
			---1

-- ptm cdbr + gtgt +vnpts             
			update ttkd_bsc.bangluong_dongia_202405 a
				   set  dtptm_dongia_cdbr='', dtptm_dongia_vnpts='', dtptm_dongia_cntt='', 
						 tong_dtptm='', luong_dongia_cdbr='', luong_dongia_vnpts='', luong_dongia_cntt=''
						 ;
		--2

-- Tong dtthu_ ptm: (ko bao gom vnptt)                
			update ttkd_bsc.bangluong_dongia_202405 a
				   set (dtptm_dongia_cdbr,dtptm_dongia_vnpts , dtptm_dongia_cntt, tong_dtptm) =
					    (select dthu_ptm_cdbr, dthu_ptm_vnpts, dthu_ptm_cntt, nvl(dthu_ptm_cdbr,0)+nvl(dthu_ptm_vnpts,0)+nvl(dthu_ptm_cntt,0)
						 from ttkd_bsc.tonghop_dtdongia_ptm_202405
						 where ma_nv=a.ma_nv)
			    where exists (select * from ttkd_bsc.tonghop_dtdongia_ptm_202405 where ma_nv=a.ma_nv)
					;
		---3

-- he so quy doi cua tong dthu:     
			update ttkd_bsc.bangluong_dongia_202405 a set heso_qd_tong=''
					;
			--4
			
			update ttkd_bsc.bangluong_dongia_202405 a
			    set heso_qd_tong = case when ma_pb in ('VNP0702600') then 1
														else		(case when tong_dtptm>0 and dtptm_quydinh>0
																						  and tong_dtptm>=dtptm_muctieu and tong_dtptm>=1.3*nvl(dtptm_bq3t,0) then 1.15
																				when tong_dtptm>0 and dtptm_quydinh>0
																						  and tong_dtptm>=dtptm_muctieu and tong_dtptm<1.3*nvl(dtptm_bq3t,0) then 1.05
																				when (tong_dtptm>0 and dtptm_quydinh>0 and tong_dtptm>=dtptm_muctieu and dtptm_bq3t is null)
																				---thang n
																						  or exists(select 1 
																												from ttkd_bsc.dinhmuc_dthu_ptm 
																												where thang = 202405 and dt_giao_bsc=0 and ma_nv=a.ma_nv
																										)                                 
																						  or (tong_dtptm>0 and dtptm_quydinh>0 and tong_dtptm>=dtptm_quydinh and tong_dtptm<dtptm_muctieu)
																						  or nvl(dtptm_quydinh,0)=0 then 1
																				when tong_dtptm>0 and dtptm_quydinh>0 and tong_dtptm>=0.8*dtptm_quydinh and tong_dtptm<dtptm_quydinh then 0.98
																				when tong_dtptm>0 and dtptm_quydinh>0 and tong_dtptm>=0.5*dtptm_quydinh and tong_dtptm<0.8*dtptm_quydinh then 0.95
																				when tong_dtptm>0 and dtptm_quydinh>0 and tong_dtptm<0.5*dtptm_quydinh then 0.85                          
																		  end)
															end
			    where tong_dtptm>0
		;
		---5
                    

			update ttkd_bsc.bangluong_dongia_202405 a set luong_dongia_cdbr='', luong_dongia_vnpts='', luong_dongia_cntt=''
			;
			--6
			update ttkd_bsc.bangluong_dongia_202405 a 
			    set luong_dongia_cdbr = case when dtptm_dongia_cdbr>0 then round(nvl(dtptm_dongia_cdbr,0)*nvl(heso_qd_tong,1)*0.858 ,0) end
					,luong_dongia_vnpts = case when dtptm_dongia_vnpts>0 then round(nvl(dtptm_dongia_vnpts,0)*nvl(heso_qd_tong,1)*0.858 ,0) end
					,luong_dongia_cntt = case when dtptm_dongia_cntt>0 then round(nvl(dtptm_dongia_cntt,0)*nvl(heso_qd_tong,1)*0.858 ,0) end
					; 
					
				--	select ma_nv, luong_dongia_cntt, luong_dongia_vnpts, luong_dongia_cdbr from ttkd_bsc.bangluong_dongia_202405 a
		---7
commit;


-- vnptt:
		update ttkd_bsc.bangluong_dongia_202405 a
		    set  ctvxhh_qly_ptr_ctv='',
				  luong_dongia_dnhm_vnptt='', 
				  luong_dongia_goi_kpbdb='',
				  luong_dongia_goi_hcm='', 
				  luong_dongia_goi_qldb='',
				  luong_dongia_vnptt=''
				  ;
			---8
 
                           
			update ttkd_bsc.bangluong_dongia_202405 a
			    set (ctvxhh_qly_ptr_ctv,luong_dongia_dnhm_vnptt, luong_dongia_goi_kpbdb, luong_dongia_goi_hcm, luong_dongia_goi_qldb,luong_dongia_vnptt)  =             
								  (select sum(luong_dongia_ctv_xhh), sum(luong_dongia_dnhm), sum(luong_dongia_goi_kpbdb), sum(luong_dongia_goi_hcm), sum(luong_dongia_goi_qldb),
											 sum(luong_dongia_vnptt_tong)            
								   from ttkd_bsc.bangluong_dongia_202405_vnptt 
								   where ma_nv=a.ma_nv)
			    where exists (select * from ttkd_bsc.bangluong_dongia_202405_vnptt where ma_nv=a.ma_nv)
			;
commit;

/* -- Kiem tra vnptt
select 'bangluong_dongia_202403_vnptt' bang, 
                sum(luong_dongia_ctv_xhh), sum(luong_dongia_dnhm), sum(luong_dongia_goi_kpbdb), sum(luong_dongia_goi_hcm), sum(luong_dongia_goi_qldb),
                sum(luong_dongia_vnptt_tong),
                nvl(sum(luong_dongia_dnhm),0) + nvl(sum(luong_dongia_goi_kpbdb),0) + nvl(sum(luong_dongia_goi_hcm),0) + nvl(sum(luong_dongia_goi_qldb),0) tong        
from bangluong_dongia_202403_vnptt a
where exists(select 1 from bangluong_dongia_202403 where ma_nv=a.ma_nv)
union all
select 'bangluong_dongia_202403', sum(ctvxhh_qly_ptr_ctv), sum(luong_dongia_dnhm_vnptt), sum(luong_dongia_goi_kpbdb), sum(luong_dongia_goi_hcm), sum(luong_dongia_goi_qldb), sum(luong_dongia_vnptt) ,
            nvl(sum(luong_dongia_dnhm_vnptt),0) + nvl(sum(luong_dongia_goi_kpbdb),0) + nvl(sum(luong_dongia_goi_hcm),0) + nvl(sum(luong_dongia_goi_qldb),0)
from bangluong_dongia_202403;
*/


-- Luong don gia ghtt + Dong luc ghtt : Nhu Y
 create table ttkd_bsc.bangluong_dongia_202405_l3 as select * from  ttkd_bsc.bangluong_dongia_202405;
 
			drop table ttkd_bsc.temp_ghtt purge;
			create table ttkd_bsc.temp_ghtt as 
			select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202405
			;
create index ttkd_bsc.temp_ghtt_makpi on ttkd_bsc.temp_ghtt (ma_kpi,loai_tinh,ma_nv);

-----BEGIN Bsung tam 1 thang 5
			rename bangluong_dongia_202404 to x_bangluong_dongia_202404_L1;
		create table ttkd_bsc.bangluong_dongia_202404 as 
				select LUONG_DONGIA_GHTT_bsT4
				from ttkd_bsc.bangluong_dongia_202404_L1
		;
		update ttkd_bsc.bangluong_dongia_202404 set LUONG_DONGIA_GHTT_bsT4 = null
		;
		
		----Luong bsung Thang 4, NSU giu lai 1 phan so voi 127750646, khong chi 3 KHDN
		update ttkd_bsc.bangluong_Dongia_202404 a 
				set  LUONG_DONGIA_GHTT_bsT4 = (select sum(tien) 
																				from  (select sum(tien) tien, ma_nv 
																								from ttkd_bsc.tl_giahan_tratruoc 
																							   where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ( 'DONGIA_TS_TP_TT')
																							   group by ma_nv 
																							   having  sum(tien)  <> 0 
																							   union all 
																									   select sum ( tien), b.ma_nv  
																									   from ttkd_bsc.tl_giahan_tratruoc B
																												  join ttkd_Bsc.nhanvien_202404 C ON B.MA_NV = c.MA_NV
																										where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ( 'DONGIATRA_OB')
																														    and c.ma_vtcv !='VNP-HNHCM_BHKV_47'
																											   group by b.ma_nv 
																											   having  sum(tien)  <> 0 
																								union all
																										select sum(tien), ma_nv 
																										from ttkd_bsc.tl_giahan_tratruoc
																										where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS')
																											group by ma_nv having  sum(tien) <> 0
																					) 
																		where ma_nv = a.ma_nv)
		where ma_donvi not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
		;
			update ttkd_bsc.bangluong_dongia_202404
		    set tong_luong_dongia= round(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
																    +nvl(luong_dongia_vnptt,0)+nvl(ctvxhh_qly_ptr_ctv,0)
																    +nvl(luong_dongia_ghtt,0)
																    +nvl(luong_khac,0)+nvl(ghtt_vnpts,0) + nvl(LUONG_DONGIA_GHTT_BST4, 0)
																  
																, 0)
		;
		
		
		update ttkd_bsc.bangluong_dongia_202404
		    set tong_luong_dongia='' 
		    where tong_luong_dongia=0
		    ;
		commit;
		;
-----END Bsung tam 1 thang 5

		---KIEMTRA
				select 'tong' tong, sum(luong_dongia_ghtt) luong_dongia_ghtt, 0 LUONG_DONGIA_GHTT_bsT4
								, sum(giamtru_ghtt_cntt) giamtru_ghtt_cntt, sum(tong_luong_dongia), SUM(thuhoi_dongia_ghtt) thuhoi_dongia_ghtt, sum(tong_luong_thuhoi)
				from ttkd_bsc.bangluong_dongia_202405 a 
				union all
				select 'ghtt_cong', sum(tien) luong_dongia_ghtt, 0,0, 0, 0, 0
					from ttkd_bsc.temp_ghtt
					where ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB' ,'DONGIA_TS_TP_TT', 'DONGIATRA_OB_BS')
					and  tien <> 0
				union all
				select 'ghtt_tru_cntt', 0, 0, sum(tien) giamtru_ghtt_cntt, 0, 0, 0
						from ttkd_bsc.temp_ghtt
						where ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA'
						and  tien <> 0
				union all
				select 'ghtt_tru_ghtt', 0,0, 0, 0, sum(tien) luong_dongia_ghtt, 0
					from ttkd_bsc.temp_ghtt
					where ma_kpi = 'DONGIA' and loai_tinh in ('THUHOI_DONGIA_GHTT')
					and  tien <> 0
					
				union all
				select 'tong T4' tong, sum(luong_dongia_ghtt) luong_dongia_ghtt, sum(LUONG_DONGIA_GHTT_bsT4) LUONG_DONGIA_GHTT_bsT4
								, sum(giamtru_ghtt_cntt) giamtru_ghtt_cntt, sum(tong_luong_dongia), 0, sum(tong_luong_thuhoi)
				from ttkd_bsc.bangluong_dongia_202404 a 
				union all
				select 'ghtt_cong_bs_t4', 0, sum(tien), 0, 0, 0, 0
																		from  (select sum(tien) tien, ma_nv 
																						from ttkd_bsc.tl_giahan_tratruoc 
																					   where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ( 'DONGIA_TS_TP_TT')
																					   group by ma_nv 
																					   having  sum(tien)  <> 0 
																					   union all 
																							   select sum ( tien), b.ma_nv  
																							   from ttkd_bsc.tl_giahan_tratruoc B
																										  join ttkd_Bsc.nhanvien_202404 C ON B.MA_NV = c.MA_NV
																								where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ( 'DONGIATRA_OB')
																												    and c.ma_vtcv !='VNP-HNHCM_BHKV_47'
																									   group by b.ma_nv 
																									   having  sum(tien)  <> 0 
																						union all
																								select sum(tien), ma_nv 
																								from ttkd_bsc.tl_giahan_tratruoc
																								where thang = 202405 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS')
																									group by ma_nv having  sum(tien) <> 0
																					) 
																		
				;
	-----

    UPDATE ttkd_bsc.bangluong_dongia_202405 SET thuhoi_dongia_ghtt = NULL, luong_dongia_ghtt = NULL, giamtru_ghtt_cntt = NULL
		; 
		ROLLBACK;
            
update ttkd_bsc.bangluong_dongia_202405 a 
    set 
		luong_dongia_ghtt = (select sum(tien) 
												from ttkd_bsc.temp_ghtt
												where ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB', 'DONGIA_TS_TP_TT')
																										 and ma_nv = a.ma_nv 
													group by ma_nv having  sum(tien) <> 0) 
								   
		, giamtru_ghtt_cntt = (select -sum(tien) from ttkd_bsc.temp_ghtt
                                                            where ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_nv = a.ma_nv
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
		, thuhoi_dongia_ghtt = (select -sum(tien) 
												from ttkd_bsc.temp_ghtt
												where ma_kpi = 'DONGIA' and loai_tinh in ('THUHOI_DONGIA_GHTT')
																										 and ma_nv = a.ma_nv 
																													--and a.ma_vtcv = 'VNP-HNHCM_BHKV_47'  ----chi thang sau
													group by ma_nv having  sum(tien) <> 0) 
;
SELECT * FROM ttkd_bsc.bangluong_dongia_202404 a ;
commit;
drop table ttkd_bsc.temp_ghtt;


-- Luong khac: 
			update ttkd_bsc.bangluong_dongia_202405 set luong_khac='', ghichu=' ' 
			    where luong_khac is not null;

    
 
-- Don gia gia han VNPTS: Chi Nguyen
update ttkd_bsc.bangluong_dongia_202405 a set ghtt_vnpts=''
;
		update ttkd_bsc.bangluong_dongia_202405 a
		    set ghtt_vnpts=(select round(sum(luong_dongia),0) from ttkd_bsc.ghtt_vnpts 
									 where thang=202405 and thang_giao is not null and ma_nv is not null
										    and ma_nv=a.ma_nv)
		    -- select ma_nv, ten_vtcv, ten_donvi, ghtt_vnpts from ttkd_bsc.bangluong_dongia_202405 a
		    where exists (select 1 from ttkd_bsc.ghtt_vnpts 
								  where thang=202405 and thang_giao is not null and ma_nv is not null and ma_nv=a.ma_nv)
			;

---Don gia cho NV thuc hien Nghiep vu lap ho so, giai quyet khieu nai: a Khanh, chi Hoi
			-----BEGIN Bsung tam 1 thang 5
			rename bangluong_dongia_202405 to x_bangluong_dongia_202405_L4; 
		
		create table ttkd_bsc.bangluong_dongia_202405 as 
				select MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_PB, TEN_PB, MA_TO, TEN_TO, LOAI_LD, DTPTM_DONGIA_CDBR, DTPTM_DONGIA_CNTT, DTPTM_DONGIA_CNTT_QD
				, DTPTM_DONGIA_VNPTS, DTPTM_DONGIA_VNPTS_TONG, TONG_DTPTM, DTPTM_QUYDINH, DTPTM_MUCTIEU, DTPTM_BQ3T, HESO_QD_TONG, LUONG_DONGIA_CDBR
				, LUONG_DONGIA_CNTT, LUONG_DONGIA_VNPTS, CTVXHH_QLY_PTR_CTV, LUONG_DONGIA_DNHM_VNPTT, LUONG_DONGIA_GOI_KPBDB, LUONG_DONGIA_GOI_HCM
				, LUONG_DONGIA_GOI_QLDB, LUONG_DONGIA_VNPTT, LUONG_DONGIA_GHTT, LUONG_DONGIA_GHTT luong_dongia_nghiepvu
				, GHTT_VNPTS, LUONG_KHAC, TONG_LUONG_DONGIA, GHICHU, LUONG_DONGIA_PTM_THUHOI
				, THUHOI_DONGIA_GHTT, GIAMTRU_HOSOTAINHA, GIAMTRU_PHATHUY_QLDB, GIAMTRU_GHTT_CNTT, LUONG_DONGIA_KHAC_THUHOI, TONG_LUONG_THUHOI
				from ttkd_bsc.x_bangluong_dongia_202405_L4
		;
		update ttkd_bsc.bangluong_dongia_202405 Set luong_dongia_nghiepvu = null
		;
			update ttkd_bsc.bangluong_dongia_202405 a
						    Set luong_dongia_nghiepvu = (    select distinct TONGTIEN from khanhtdt_ttkd.TONGHOP_BSC_KPI_2024 b
																			 where b.thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
																				  and b.ma_kpi in ('HCM_SL_HOTRO_004')					-- <== L?U Ý MÃ KPI
																						 and b.loai_kpi = 'KPI_NV'                             -- <== L?U Ý
																				  and b.ma_nv = a.ma_nv
																		 )
			where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv
										where ma_kpi in ('HCM_SL_HOTRO_004')										-- <== L?U Ý MÃ KPI
											  and thang_kt is null
											  and giamdoc_phogiamdoc is null
											  and MA_VTCV = a.MA_VTCV )
			;
		
 commit; 
-- Tong luong don gia:  chay dot 1, dot 2
--		update ttkd_bsc.bangluong_dongia_202405 set tong_luong_dongia='' where tong_luong_dongia is not null
		;
		update ttkd_bsc.bangluong_dongia_202405 a
						set luong_khac = nvl(luong_khac, 0) + (select TIEN_BS_202405 from ttkd_bsc.bangluong_dongia_202404_l4_bs_202405_2nv where ma_nv = a.ma_nv)
		where ma_nv in (select ma_nv from ttkd_bsc.bangluong_dongia_202404_l4_bs_202405_2nv)
						;
		select * from ttkd_bsc.bangluong_dongia_202404_l4_bs_202405_2nv;
		;
		update ttkd_bsc.bangluong_dongia_202405
		    set tong_luong_dongia= round(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
																    +nvl(luong_dongia_vnptt,0)+nvl(ctvxhh_qly_ptr_ctv,0)
																    +nvl(luong_dongia_ghtt,0)
																    +nvl(luong_khac,0)+nvl(ghtt_vnpts,0) 
																    + nvl(luong_dongia_nghiepvu, 0)
																, 0)
		;
		
		
		update ttkd_bsc.bangluong_dongia_202405
		    set tong_luong_dongia='' 
		    where tong_luong_dongia=0
		    ;
     --    select * from ttkd_bsc.bangluong_dongia_202405 where ma_nv in ('VNP017190', 'VNP027259')

commit;

---da xong
    
    
/* -- Kiem tra tong luong don gia: 
			TONG_LUONG_DONGIA=LUONG_DONGIA_CDBR+LUONG_DONGIA_CNTT+LUONG_DONGIA_VNPTS+CTVXHH_QLY_PTR_CTV+LUONG_DONGIA_DNHM_VNPTT+LUONG_DONGIA_GOI_KPBDB+LUONG_DONGIA_GOI_HCM+LUONG_DONGIA_GOI_QLDB+LUONG_DONGIA_GHTT+DONGLUC_GHTT+DONGLUC_GHTS+GHTT_VNPTS+LUONG_KHAC
			
			select sum(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
							+nvl(luong_dongia_vnptt,0)+nvl(ctvxhh_qly_ptr_ctv,0) 
							+nvl(luong_dongia_ghtt,0)+nvl(dongluc_ghtt,0)+nvl(dongluc_ghts,0)
							+nvl(luong_khac,0)+nvl(ghtt_vnpts,0)
						 ) tong
			    from  ttkd_bsc.bangluong_dongia_202405
			union all
			select sum(tong_luong_dongia) from ttkd_bsc.bangluong_dongia_202405;

*/

select sum(nvl(luong_dongia_vnptt,0)
																    ) a from ttkd_bsc.bangluong_dongia_202405;
            
-- THU HOI + PHAT HUY NVPTM: 
			update ttkd_bsc.bangluong_dongia_202405 a
						 set  
								 giamtru_hosotainha='', giamtru_phathuy_qldb=''--, giamtru_ghtt_cntt=''
								, luong_dongia_ptm_thuhoi=''--, thuhoi_dongia_ghtt = null
								, luong_dongia_khac_thuhoi=''
								, tong_luong_thuhoi = null
				;
			select *  from ttkd_bsc.bangluong_dongia_202405
			;


			update ttkd_bsc.bangluong_dongia_202405 a  
			   set luong_dongia_ptm_thuhoi = '' where luong_dongia_ptm_thuhoi is not null
			   ;
   
				update ttkd_bsc.bangluong_dongia_202405 a  
				   set luong_dongia_ptm_thuhoi = (select sum(tien_thuhoi) from ttkd_bsc.ct_thuhoi where thang=202405 and ma_nv=a.ma_nv)
				   where exists(select 1 from ttkd_bsc.ct_thuhoi where thang=202405 and ma_nv=a.ma_nv);

 
-- giam tru do thu ho so tai nha: 
			update ttkd_bsc.bangluong_dongia_202405 a set giamtru_hosotainha=''
			;
			update ttkd_bsc.bangluong_dongia_202405 a
				   set giamtru_hosotainha=
					   (select round(sum(tien_giam) ,0) from ttkd_bsc.ct_giamtru where thang=202405 and ma_nv=a.ma_nv)
			    --  select giamtru_hosotainha from ttkd_bsc.bangluong_dongia_202403  a
			    where exists (select 1 from ttkd_bsc.ct_giamtru where thang=202405 and ma_nv=a.ma_nv);



-- giam tru phat huy qldb: 
			drop table ttkd_bsc.temp_phathuy;
			create table ttkd_bsc.temp_phathuy as select * from ttkd_bsc.ct_bsc_phathuy where thang = 202405
			;
			create index temp_phathuy_thang on temp_phathuy (thang);
			create index temp_phathuy_manvcskh on temp_phathuy (manv_cskh);
			
			update ttkd_bsc.bangluong_dongia_202405 a set giamtru_phathuy_qldb='' where giamtru_phathuy_qldb is not null
			;
			update ttkd_bsc.bangluong_dongia_202405 a 
				   set giamtru_phathuy_qldb = (select round(sum(tien_nvcskh) ,0) from ttkd_bsc.temp_phathuy
															where thang = 202405 and manv_cskh = a.ma_nv
															group by manv_cskh)
			    where exists (select 1 from ttkd_bsc.temp_phathuy where thang = 202405 and manv_cskh = a.ma_nv);
			commit;                                                                                                                                                                        



-- tong_luong_thuhoi:
update ttkd_bsc.bangluong_dongia_202405 a set tong_luong_thuhoi='' where tong_luong_thuhoi is not null
;
			update ttkd_bsc.bangluong_dongia_202405 a
			   set tong_luong_thuhoi=round( nvl(luong_dongia_ptm_thuhoi,0) +  nvl(luong_dongia_khac_thuhoi,0)
																	+ nvl(giamtru_hosotainha,0) +  nvl(giamtru_phathuy_qldb,0) 
																	+ nvl(giamtru_ghtt_cntt,0)  + nvl(thuhoi_dongia_ghtt, 0)
															, 0)
															;
															
			update ttkd_bsc.bangluong_dongia_202405 a
			   set tong_luong_thuhoi='' where tong_luong_thuhoi=0;
			   
			commit;


			/*
			-- Kiem tra thu hoi:
			select '4 field tien thu hoi' field, 
					  round( sum (nvl(luong_dongia_ptm_thuhoi,0)  +  nvl(luong_dongia_khac_thuhoi,0)
					  +  nvl(giamtru_hosotainha,0)   +  nvl(giamtru_phathuy_qldb,0) 
					   ) ,0)      tien_thuhoi
			from ttkd_bsc.bangluong_dongia_202405
			union all
			select 'tien_luong_thuhoi', sum(tong_luong_thuhoi) from ttkd_bsc.bangluong_dongia_202405;
			*/


-- XUAT GUI BANG LUONG DON GIA: gui NSU
	select sum(tong_luong_dongia) - sum(luong_dongia_ghtt_bst4) tong_luong_dongia_l1, sum(tong_luong_dongia) tong_luong_dongia_l2, sum(tong_luong_thuhoi) tong_luong_thuhoi, sum(luong_dongia_ghtt_bst4) luong_dongia_ghtt_bst4 from ttkd_bsc.bangluong_dongia_202404; 
     	select sum(tong_luong_dongia) tong_luong_dongia, sum(tong_luong_thuhoi) tong_luong_thuhoi
		from ttkd_bsc.bangluong_dongia_202405; 
	   -- 1 707 061 451  -- 04/04/2024
        -- 6926474775.9688 -- 21/04/2024
        -- 6882915262.9688 -- 22/04/2024
	   
	   --1 697 828 966  12g48 05/05/2024
        -- 5542289126.376 -----257756959 ---20g 20/05/2024
	   -- 5542289126.376	---- 257503959 ---21g 21/05/2024
	   -- 5544524128.376	-----257085459   ---17g30 ngay 22/05/24
	  
	   -- 2 385 280 621		19g43 4/6/24
	   -- 5 966 268 200.4072	381 428 543
	   -- 6 202 595 591	329 513 589
	   --6 138 557 114	302 772 597
	   -- 6 138 557 114	329 513 589
	   -- 6 189 389 389	329 513 589
	   --6 052 201 950	329 513 589
	   -- 6 059 753 520	329 513 589
	   
	----NSu tren 100tr gui NSU
		select ma_nv, ten_nv, ten_pb, tong_luong_dongia from ttkd_bsc.bangluong_dongia_202405 where tong_luong_dongia>100000000
		;
		select * from ttkd_bsc.ct_bsc_ptm where thang_tldg_dt=202404 and manv_ptm='VNP027927';
		
	---gui chi tiet NSU
		select ma_nv, ten_nv,ma_vtcv,ten_vtcv,	ma_pb,	ten_pb,	ma_to,ten_to,	loai_ld,
				  dtptm_dongia_cdbr,	dtptm_dongia_vnpts_tong,	dtptm_dongia_vnpts,	
				  dtptm_dongia_cntt, tong_dtptm, dtptm_quydinh, dtptm_muctieu,	dtptm_bq3t,
				  heso_qd_tong,	luong_dongia_cdbr,	luong_dongia_cntt, luong_dongia_vnpts, 
				  luong_dongia_dnhm_vnptt, luong_dongia_goi_kpbdb, luong_dongia_goi_hcm, luong_dongia_goi_qldb,
				  luong_dongia_vnptt, ctvxhh_qly_ptr_ctv, ghtt_vnpts,	luong_khac,
				  luong_dongia_ghtt, luong_dongia_nghiepvu, tong_luong_dongia,	ghichu	,
				  luong_dongia_ptm_thuhoi,	luong_dongia_khac_thuhoi, thuhoi_dongia_ghtt
				  , giamtru_hosotainha,	giamtru_phathuy_qldb, giamtru_ghtt_cntt, tong_luong_thuhoi	
		from ttkd_bsc.bangluong_dongia_202405
		;

select * from ttkd_bsc.bangluong_dongia_202405;
select * from ttkd_bsc.bangluong_kpi_202405 ;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB';

select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO
			, HCM_DT_PTMOI_021
			,HCM_DT_PTMOI_044
			,HCM_DT_PTMOI_052
			, HCM_SL_BANLE_017
			, HCM_DT_PTMOI_053
			, HCM_CL_TNGOI_004, HCM_SL_ORDER_001
from ttkd_bsc.bangluong_kpi_202405 where ma_nv = 'CTV083351';

SELECT ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb,ma_to, ten_to,ma_kpi, ten_kpi, to_char(giatri) giatri
  FROM ttkd_bsc.bangluong_kpi_202405_tracuu where ma_nv = 'VNP020231';
  ;

			----Gui NSU nvien khong ton tai trong bang nhanvien
			select ma_nv, 
				    sum(luong_ptm_cdbr) luong_ptm_cdbr, 
				    sum(luong_ptm_vnpts) luong_ptm_vnpts, 
				    sum(luong_ptm_vnptt) luong_ptm_vnptt, 
				    sum(luong_ptm_cntt) luong_ptm_cntt
			from (
					select ma_nv, dthu_ptm_cdbr, dthu_ptm_vnpts, dthu_ptm_vnptt, dthu_ptm_cntt, luong_ptm_cdbr, luong_ptm_vnpts, luong_ptm_vnptt, luong_ptm_cntt 
					  from ttkd_bsc.tonghop_dtdongia_ptm_202405 a 
					  where-- ma_to is null 
--								 and not exists(select * from ttkd_bsc.nhanvien where thang = 202405 and ma_nv=a.ma_nv)
--								 and not exists(select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang >= 202301 and ma_nv=a.ma_nv)
								-- and 
								 not exists(select 1 from ttkd_bsc.nhanvien where thang = 202405 and ma_nv = a.ma_nv)
								 and exists (select 1 from ttkd_bsc.nhanvien where thang <= 202404 and donvi = 'TTKD' and ma_nv = a.ma_nv)
								 and nvl(luong_ptm_cdbr,0)+nvl(luong_ptm_vnpts,0)+nvl(luong_ptm_vnptt,0)+nvl(luong_ptm_cntt,0)>0 
								 and ma_nv not in ('bhgd','bhcc')
					union all
							select ma_nv, 0,0,0,0,0,0,  sum(nvl(luong_dongia_ctv_xhh,0)) + sum(nvl(luong_dongia_vnptt_tong,0))  ,0
							  from ttkd_bsc.bangluong_dongia_202405_vnptt a
							  where 
--											not exists(select * from ttkd_bsc.bangluong_dongia_202405 where ma_nv=a.ma_nv)
--											and not exists(select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang >= 202301 and  ma_nv=a.ma_nv)
											 not exists (select 1 from ttkd_bsc.nhanvien where thang = 202405 and ma_nv = a.ma_nv)
											and exists (select 1 from ttkd_bsc.nhanvien where thang <= 202404 and donvi = 'TTKD'  and ma_nv = a.ma_nv)
							  group by ma_nv)
			group by ma_nv
			  ;



-- chi tiet gui anh Nghia:
			select * from ttkd_bsc.ct_bsc_ptm 
			    where( (thang_ptm>=202402 and (thang_tldg_dt=202405 or thang_tldg_dt is null)) or (thang_ptm<202402 and thang_tldg_dt=202405)
			    )
						and manv_ptm in ('VNP030420')--, 'VNP028832', 'VNP027927')
			    ; 
			 
			;
			select * from ttkd_bsc.ct_bsc_ptm_pgp 
			    where (thang_ptm>=202402 and (thang_tldg_dt_nvhotro=202405 or thang_tldg_dt_nvhotro is null)) 
					  or (thang_ptm<202402 and (thang_tlkpi_hotro=202405 or thang_tldg_dt_nvhotro=202405)  )
					  ; 


-- Kiem tra dnhm:
select manv_ptm,dich_vu,tien_dnhm, a.HESO_DICHVU_DNHM,DOANHTHU_DONGIA_DNHM, DOANHTHU_KPI_DNHM, DOANHTHU_KPI_DNHM_PHONG,
            thang_tldg_dnhm, thang_tlkpi_dnhm, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG
		  , lydo_khongtinh_luong, lydo_khongtinh_dongia
from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202404 and dichvuvt_id not in (2,13,14,15,16) and tien_dnhm>0 and ngay_tt is not null and thang_tldg_dnhm is null;

select * from ttkd_bsc.bangluong_dongia_202405;


-- GUI PNS : PTM
select a.ten_pb, a.ten_to, a.ma_nv,  a.ten_nv, a.ten_vtcv
            ,b.tong_dtptm tong_dtptm_old, b.heso_qd_tong heso_qd_tong_old
            ,a.tong_dtptm tong_dtptm_new, a.heso_qd_tong heso_qd_tong_new
            ,a.tong_luong_dongia tong_luong_dongia_new, b.tong_luong_dongia tong_luong_dongia_old                     
           ,nvl(b.luong_dongia_cdbr,0)+nvl(b.luong_dongia_vnpts,0)+nvl(b.luong_dongia_cntt,0) +nvl(b.luong_dongia_vnptt,0) tongluong_ptm_old
            ,nvl(a.luong_dongia_cdbr,0)+nvl(a.luong_dongia_vnpts,0)+nvl(a.luong_dongia_cntt,0)+nvl(a.luong_dongia_vnptt,0) tongluong_ptm_new                         
             ,nvl(a.tong_luong_dongia,0) - nvl(b.tong_luong_dongia,0) chechlech
		   , b.luong_dongia_ghtt  luong_dongia_ghtt_old, a.luong_dongia_ghtt luong_dongia_ghtt_new
from ttkd_bsc.bangluong_dongia_202405 a, ttkd_bsc.bangluong_dongia_202405_l3 b 
where a.ma_nv=b.ma_nv and nvl(a.tong_luong_dongia,0)<>nvl(b.tong_luong_dongia,0)       
order by (a.tong_luong_dongia - b.tong_luong_dongia);


select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ten_vtcv
             ,a.tong_luong_thuhoi tong_luong_thuhoi_new, b.tong_luong_thuhoi tong_luong_thuhoi_old,
             nvl(a.tong_luong_thuhoi,0) - nvl(b.tong_luong_thuhoi,0) chechlech

  from ttkd_bsc.bangluong_dongia_202405 a, ttkd_bsc.bangluong_dongia_202405_l3 b 
where a.ma_nv=b.ma_nv 
            and nvl(a.tong_luong_thuhoi,0)<>nvl(b.tong_luong_thuhoi,0)       
order by (a.tong_luong_thuhoi - b.tong_luong_thuhoi);


-- ktra 1 nhan vien:
select ma_gd, ma_tb, tien_dnhm DTHU_DONGIA, luong_dongia_dnhm_nvptm LUONG_DONGIA, thang_tldg_dnhm thang_tldg, 'dnhm' LOAI_TIEN
    from ttkd_bsc.ct_bsc_ptm
    where thang_tldg_dnhm=202403 and manv_ptm='CTV073028'
union all
select ma_gd, ma_tb, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt thang_tldg , 'dthu_goi'
    from ttkd_bsc.ct_bsc_ptm
    where thang_tldg_dt=202403 and manv_ptm='CTV073028';
    
    
    --Khieu nai thang 5
       ---khoa ds chi 9 tbao nay trong thang 5, chua chi (da xu ly xong, 2 tbao tinh héo dvu 0.3, con 7 tinh T9 giu nguyen heso quy dinh
			    update  ttkd_bsc.ct_bsc_ptm set thang_luong = 9999, THANG_TLDG_DT = 202405, THANG_TLKPI = 202405, THANG_TLKPI_TO = 202405, THANG_TLKPI_PHONG = 202405--, heso_dichvu = 0.3
--					select * from 	ttkd_bsc.ct_bsc_ptm
						where ma_gd = 'HCM-LD/01579295' and manv_ptm in ('VNP030420') --and ma_tb in ('hcm_colo_00010720', 'hcm_colo_00010838', 'hcm_colo_00010836')
			    ;
			    commit;
			    rollback;
   


