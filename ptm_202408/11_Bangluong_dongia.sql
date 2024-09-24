create table ttkd_bsc.tonghop_dtdongia_ptm_202404_l1 as select * from ttkd_bsc.tonghop_dtdongia_ptm_202404;
create table ttkd_bsc.bangluong_dongia_202404_l5 as select * from ttkd_bsc.bangluong_dongia_202404; 
create table ttkd_bsc.bangluong_dongia_202408_dot3 as select * from ttkd_bsc.bangluong_dongia_202408; 
rename bangluong_dongia_202405 to bangluong_dongia_202405_new;
rename bangluong_dongia_202406_l4 to bangluong_dongia_202406;
select * from ttkd_bsc.bangluong_dongia_202404_l4_bs_202405_2nv;
select sum(LUONG_DONGIA_DNHM_VNPTT), sum(LUONG_DONGIA_GOI_VNPTT) from ttkd_bsc.bangluong_dongia_202408_dot2; 283634249	166502863

---lan dau tien chay doan Frame nay, lan sau khong chay doan FRAME nay
------BEGIN dot 1---khong chay cho dot 2
drop table ttkd_bsc.bangluong_dongia_202408;
				create table ttkd_bsc.bangluong_dongia_202408 (
				donvi 		varchar(20),
				ma_nv               varchar(10),
				ten_nv               varchar(50),
				ma_vtcv            varchar(20),
				ten_vtcv            varchar(100),
				ma_pb         varchar(10),
				ten_pb         varchar(50),
				ma_to               varchar(10),
				ten_to               varchar(100),
				loai_ld               varchar(50),
				nhomld_id               number,

				dtptm_dongia_cdbr        number,
				dtptm_dongia_cntt         number,
				dtptm_dongia_vnpts      number,
				dtptm_dongia_vnptt   number,
				tong_dtptm                     number,
				
				luong_dongia_cdbr        number,
				luong_dongia_cntt         number,
				luong_dongia_vnpts      number,
				
				luong_dongia_dnhm_vnptt   number, 
				luong_dongia_goi_vnptt         number,
				
				luong_dongia_ghtt           number,
				luong_dongia_nghiepvu           number,
				luong_dongia_chungtu number,
				luong_dongia_thucuoc number,
				ghtt_vnpts                         number,
				luong_khac                       number
				, luong_dongia_ptm_bs_t7   number
				, tong_luong_dongia         number,
				
				ghichu                               varchar2(500),
				luong_dongia_ptm_thuhoi number,
				thuhoi_dongia_ghtt number,
				giamtru_hosotainha               number,
				giamtru_ghtt_cntt				number,
				luong_dongia_khac_thuhoi    number
				, luong_dongia_ptm_thuhoi_t7     number
				, tong_luong_thuhoi                number
				, tong_thulao_thucchi			number
				)
	;


			insert into ttkd_bsc.bangluong_dongia_202408
				 (donvi, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld, nhomld_id)
			SELECT donvi, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld, nhomld_id
			  FROM ttkd_bsc.nhanvien
			  where thang = 202408
			  ; 
		commit;

-- Doanh thu dinh muc - 268 /TTr- NS 05/09/2022 
	---Thay the Doanh thu dinh muc theo vb 243 TTr NSU DH KHKT 20240812 eO
				---vb moi 323 TTr NSU DH KHKT 20240812 eO thay the b 243 
		-- Update neu co thay doi
			insert into ttkd_bsc.bldg_danhmuc_vtcv_p1 (MA_VTCV, TEN_VTCV, THANG_BD, DINHMUC_1, DINHMUC_2, VANBAN, GHICHU)
				select distinct ma_vtcv, ten_vtcv, thang thang_bd
							, case ma_vtcv 	when 'VNP-HNHCM_BHKV_6' then 13000000
														when 'VNP-HNHCM_BHKV_15' then 13000000 
														when 'VNP-HNHCM_BHKV_22' then 8000000
														when 'VNP-HNHCM_BHKV_27' then 5000000 
														when 'VNP-HNHCM_KDOL_4' then 15000000
									end dinhmuc_1
							, case ma_vtcv 	when 'VNP-HNHCM_BHKV_6' then 10000000
														when 'VNP-HNHCM_BHKV_15' then 10000000 
														when 'VNP-HNHCM_BHKV_22' then 5000000
														when 'VNP-HNHCM_BHKV_27' then 4000000 
														when 'VNP-HNHCM_KDOL_4' then 12000000 
									end dinhmuc_2, '243 TTr NSU DH KHKT 20240812 eO'VANBAN, 'doanh thu dinh muc chuan: vnd' GHICHU
				from ttkd_bsc.nhanvien
				where donvi = 'TTKD' and thang = 202408 and ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_22', 'VNP-HNHCM_BHKV_27', 'VNP-HNHCM_KDOL_4')
		;
		
		select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202408; and DINHMUC_1 is not null
		;
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202408
		;
		 select * from ttkd_bsc.bangluong_dongia_202408;



------END dot 1---khong chay cho dot 2
-- he so quy doi cua tong dthu:     
			---Ap dung vb 323 dv BHKV, 292 BHDN
			
			select thang, ma_nv from ttkd_bsc.bang_heso_dthu group by thang, ma_nv having count(*)>1;
			select * from ttkd_bsc.bang_heso_dthu where thang = 202408;
			
			delete from ttkd_bsc.bang_heso_dthu where thang = 202408 and donvi = 'TTKD';
			insert into ttkd_bsc.bang_heso_dthu  
						select thang, DONVI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_PB, TEN_PB, MA_TO, TEN_TO, LOAI_LD, nhomld_id, null heso_dthu
							from ttkd_bsc.nhanvien where thang = 202408 and donvi = 'TTKD'
						;
			---chi chay tam dot 1 tam ung luong, qua dot tam ung thi xoa va chay theo KPI, ap heso = 0.8 doi voi 12 phong ban hang
			update ttkd_bsc.bang_heso_dthu a
						set heso_dthu = 0.8
				where ma_pb in ('VNP0701100',
												'VNP0701200',
												'VNP0701300',
												'VNP0701400',
												'VNP0701500',
												'VNP0701600',
												'VNP0701800',
												'VNP0702100',
												'VNP0702200',
												'VNP0702300',
												'VNP0702400',
												'VNP0702500'
										)
								and a.thang = 202408
						;
			---Chay dot 2 tu dong nay
			----Update Heso dthu tu 2 table ttkd_bsc.dinhmuc_giao_ptm va TLTH chi tieu 1 KHDN
			---Ap dung vb 323 dv BHKV		
					update ttkd_bsc.bang_heso_dthu a
						set heso_dthu = (select HESO_QD_DT_PTM from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
--			select heso_dthu from ttkd_bsc.bang_heso_dthu a 
			where --ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41', 'VNP-HNHCM_BHKV_22', 'VNP-HNHCM_BHKV_27', 'VNP-HNHCM_BHKV_15')and 
						ma_pb in ('VNP0701100',
										'VNP0701200',
										'VNP0701300',
										'VNP0701400',
										'VNP0701500',
										'VNP0701600',
										'VNP0701800',
										'VNP0702100',
										'VNP0702200',
										'VNP0702300',
										'VNP0702400',
										'VNP0702500'
										)
					and a.thang = 202408 
					;		
			
			----END
			select * from ttkd_bsc.bang_heso_dthu where thang = 202408;
			commit;
		
			---Nvien moi danh gia bsc --> heso_dthu null OR < 1 --> assign = 1, nguoc lai giu nguyen 
			update ttkd_bsc.bang_heso_dthu a set heso_dthu = 1
--			select * from ttkd_bsc.bang_heso_dthu a
			where exists (select * from ttkd_bsc.nhanvien where thang = a.thang and tinh_bsc = 0 and ma_nv = a.ma_nv)
						and heso_dthu is null
						and thang = 202408
			;
			commit;
			rollback;
		
		create table  ttkd_bsc.tonghop_ct_dongia_ptm_202408_lan1 as select * from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202408;
		select sum(HESO_DTHU) from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202408; 96948.73
		
		-----*************Desc cong thuc
		---Minh Viber nhom luong 14/08, heso vb 323 cua nv KDDTT chi ap dung tren dthu goi, khong ap dung dthu kich hoat, cac vi tri khac bthuong
--delete from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202408 ;

select sum(TIEN_THULAO) from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202408; and thang_ptm = 202408; ma_nv = 'VNP019958'; thang 7 = 2 259 806 587 ---20--5860842912
--create table ttkd_bsc.tonghop_ct_dongia_ptm as
insert into ttkd_bsc.tonghop_ct_dongia_ptm
			with nv as (select thang, donvi, ma_nv, ten_nv, ten_to, ten_pb, ma_to, ma_pb, ma_vtcv, nhomld_id
									from ttkd_bsc.nhanvien where thang = 202408
									union
									select thang, 'POT_VTTP' donvi, ma_nv, ten_nv, ten_to, ten_pb, ma_to, ma_pb, ma_vtcv, nhomld_id
									from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202408
									union
									select 202408 thang, 'POT' donvi, MA_NV, TEN_NV, TEN_dv, null, 'POT', null, null, 5 nhomld_id 
									from admin_hcm.nhanvien_onebss a
												join admin_hcm.donvi b on a.donvi_id = b.donvi_id --where a.ma_nv = 'POT0002'
									where a.donvi_id = 893845
									
								)
			select 
							THANG_DONGIA thang, nv.donvi, manv_ptm ma_nv, nv.ten_nv, nv.ten_to, nv.ten_pb, nv.ma_to, nv.ma_pb, nv.ma_vtcv, nv.nhomld_id
							, ptm_id, THANG_PTM, MA_GD, thuebao_id, MA_TB, LOAITB_ID, DICHVUVT_ID
							, doanhthu_dongia
							, dongia
							, LUONG_DONGIA_CDBR, LUONG_DONGIA_VNPTS, LUONG_DONGIA_KHAC
							, case when nguon = 'dnhm' then LUONG_DONGIA_VNPTT end LUONG_DONGIA_dnhm_VNPTT
							, case when nguon = 'nvptm' then LUONG_DONGIA_VNPTT end LUONG_DONGIA_goi_VNPTT
							, luong_dongia
							, case when loaitb_id = 21 and nguon = 'dnhm' and nv.ma_vtcv in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_17') then 1		--Chu Minh sms 2 vi tri va VNPtt dnhm gan heso = 1
										when loaitb_id = 21 and nv.donvi in ('VTTP', 'POT_VTTP', 'POT') then 1				---VTTP, POTMASCO VNPtt heso = 1
										when nv.donvi in ('VTTP', 'POT_VTTP', 'POT') and thang_ptm < 202407 then 1 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										when nv.donvi in ('VTTP', 'POT_VTTP', 'POT') and thang_ptm >= 202407 then 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										else nvl(hs.heso_dthu, 1) end heso_dthu
							, round(case when loaitb_id = 21 and nguon = 'dnhm' and nv.ma_vtcv in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_17') then luong_dongia
													when loaitb_id = 21 and nv.donvi in ('VTTP', 'POT_VTTP', 'POT') then luong_dongia
													when loaitb_id = 21 and nguon = 'nvptm' then luong_dongia * nvl(hs.heso_dthu, 1)
													when nv.donvi in ('VTTP', 'POT_VTTP', 'POT') and thang_ptm < 202407 then luong_dongia 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
													when nv.donvi in ('VTTP', 'POT_VTTP', 'POT') and thang_ptm >= 202407 then luong_dongia * 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
															else luong_dongia * nvl(hs.heso_dthu, 1)
											end, 0)  tien_thulao
							, NGUON
					from
					(
					    -- nvptm
					    select  id ptm_id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_ptm
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvptm end dthu_dongia_cdbr
									  , case when loaitb_id=20 then doanhthu_dongia_nvptm end dthu_dongia_vnpts
									  , case when loaitb_id=21  then doanhthu_dongia_nvptm end dthu_dongia_vnptt
									  , case when dichvuvt_id in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvptm end dthu_dongia_khac
									  , doanhthu_dongia_nvptm doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvptm end luong_dongia_cdbr
									  , case when loaitb_id = 20  then luong_dongia_nvptm end luong_dongia_vnpts
									  , case when loaitb_id = 21  then luong_dongia_nvptm end luong_dongia_vnptt
									  , case when dichvuvt_id in (13,14,15,16) or loaitb_id is null then luong_dongia_nvptm end luong_dongia_khac
									  , luong_dongia_nvptm luong_dongia
									  ,'nvptm' nguon, thang_tldg_dt thang_dongia, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_ptm is not null and thang_tldg_dt = 202408
					    
		union all     
					    -- dnhm cua nvhotro tao hop dong (phan chia nvien hotro DIGISHOP)
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_ptm
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dnhm * heso_hotro_nvptm end dthu_dongia_cdbr
									  , case when loaitb_id = 20 then doanhthu_dongia_dnhm * heso_hotro_nvptm end dthu_dongia_vnpts
									  , case when loaitb_id = 21 then doanhthu_dongia_dnhm end dthu_dongia_vnptt
									  , case when loaitb_id in (38,127) then doanhthu_dongia_dnhm * heso_hotro_nvptm end  dthu_dongia_khac
									  , doanhthu_dongia_dnhm * nvl(heso_hotro_nvptm, 1) doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_cdbr
									  , case when loaitb_id =20 then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_vnpts
									  , case when loaitb_id =21 then luong_dongia_dnhm_nvptm end luong_dongia_vnptt
									  , case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_khac
									  , luong_dongia_dnhm_nvptm * nvl(heso_hotro_nvptm, 1) luong_dongia
									 , 'dnhm' nguon, thang_tldg_dnhm, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_ptm is not null and thang_tldg_dnhm = 202408 
					   
		union all			    
					     -- dnhm cua nvptm (phan chia nvien tiep thi DIGISHOP)
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_hotro
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_cdbr
									  , case when loaitb_id = 20 then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_vnpts
									  , case when loaitb_id = 21 then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_vnptt
									  , case when loaitb_id in (38,127) then doanhthu_dongia_dnhm * heso_hotro_nvhotro end  dthu_dongia_khac
									  , doanhthu_dongia_dnhm * nvl(heso_hotro_nvhotro, 1) doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_cdbr
									  , case when loaitb_id = 20 then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_vnpts
									  , 0 luong_dongia_vnptt
									  , case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_khac
									  , luong_dongia_dnhm_nvptm * nvl(heso_hotro_nvhotro, 1) luong_dongia
									 , 'dnhm' nguon, thang_tldg_dnhm, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_hotro is not null and thang_tldg_dnhm = 202408 
										and tyle_hotro is null and tyle_am is null --and nvl(vanban_id, 0) != 764 ---only T7 xoa
					    
		union all     
					    -- nv dai
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_tt_dai
							  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dai end dthu_dongia_cdbr
							  , case when loaitb_id=20  then doanhthu_dongia_dai end dthu_dongia_vnpts
							  , case when loaitb_id=21  then doanhthu_dongia_dai end dthu_dongia_vnptt
							  , case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_dai end dthu_dongia_khac
							  , doanhthu_dongia_dai doanhthu_dongia
							  , dongia
							  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dai end luong_dongia_cdbr
							  , case when loaitb_id=20  then luong_dongia_dai end luong_dongia_vnpts
							 , case when loaitb_id = 21  then luong_dongia_dai end luong_dongia_vnptt
							  , case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_dai end luong_dongia_khac
							  , luong_dongia_dai luong_dongia
							, 'nvtt_dai' nguon, thang_tldg_dt_dai, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_tt_dai is not null and thang_tldg_dt_dai = 202408
		
		union all     
					    -- nv gioi thieu DIGISHOP, ngoai tru  phong GP
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_hotro
										  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvhotro end dthu_dongia_cdbr
										  , case when loaitb_id=20  then doanhthu_dongia_nvhotro end dthu_dongia_vnpts
										  , case when loaitb_id=21  then doanhthu_dongia_nvhotro end dthu_dongia_vnptt
										  , case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvhotro end dthu_dongia_khac
										  , doanhthu_dongia_nvhotro doanhthu_dongia
										  , dongia
										  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvhotro end luong_dongia_cdbr
										  , case when loaitb_id=20  then luong_dongia_nvhotro end luong_dongia_vnpts
										 , case when loaitb_id=21  then luong_dongia_nvhotro end luong_dongia_vnptt
										  , case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_nvhotro end luong_dongia_khac
										  , luong_dongia_nvhotro luong_dongia
										,'nvtt_gioithieu' nguon, thang_tldg_dt_nvhotro, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm a
					    where manv_hotro is not null and thang_tldg_dt_nvhotro = 202408 
									and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202408 and ma_pb = 'VNP0702600' and ma_nv = a.manv_hotro)
		union all     
					    -- nv ho tro cua PGP
					    select  ptm_id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_hotro
							  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvhotro end dthu_dongia_cdbr
							  , case when loaitb_id=20  then doanhthu_dongia_nvhotro end dthu_dongia_vnpts
							  , case when loaitb_id=21  then doanhthu_dongia_nvhotro end dthu_dongia_vnptt
							  , case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvhotro end dthu_dongia_khac
							  , doanhthu_dongia_nvhotro doanhthu_dongia
							  , dongia
							  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvhotro end luong_dongia_cdbr
							  , case when loaitb_id=20  then luong_dongia_nvhotro end luong_dongia_vnpts
							 , case when loaitb_id = 21  then luong_dongia_nvhotro end luong_dongia_vnptt
							  , case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_nvhotro end luong_dongia_khac
							  , luong_dongia_nvhotro luong_dongia
							, 'manv_hotro' nguon, thang_tldg_dt_nvhotro, to_number(thang_ptm)
					    from ttkd_bsc.ct_bsc_ptm_pgp
					    where manv_hotro is not null and thang_tldg_dt_nvhotro = 202408
			    ) a
			    
			left join nv on nv.thang = a.thang_dongia and nv.ma_nv = a.manv_ptm
			left join ttkd_bsc.bang_heso_dthu hs on hs.thang = a.thang_ptm and a.manv_ptm = hs.ma_nv
			where manv_ptm is not null 
			; 
		

			
	----all dvu ngoai tru VNPtt
			select sum(LUONG_DONGIA) from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202408 and loaitb_id = 21;  ---490779496 --529 594 190
			;
			update ttkd_bsc.bangluong_dongia_202408
						set TONG_LUONG_DONGIA = null, luong_dongia_cdbr = null
									, luong_dongia_vnpts = null, luong_dongia_cntt = null, luong_dongia_dnhm_vnptt = null, luong_dongia_goi_vnptt = null
						;
			select sum(TIEN_THULAO)--, ma_nv
						 from ttkd_bsc.tonghop_ct_dongia_ptm
						 where thang = 202408 and donvi = 'TTKD'
						 group by ma_nv
			;
			drop table ttkd_bsc.x_dongia_temp purge;
			create table ttkd_bsc.x_dongia_temp as
					select ma_nv, donvi
											, sum(case when dichvuvt_id not in (2, 13, 14, 15, 16) then DOANHTHU_DONGIA else 0 end) DTPTM_DONGIA_CDBR
											, sum(case when loaitb_id = 20 then DOANHTHU_DONGIA else 0 end) DTPTM_DONGIA_VNPTS
											, sum(case when dichvuvt_id in (13, 14, 15, 16) then DOANHTHU_DONGIA else 0 end) DTPTM_DONGIA_CNTT
											, sum(case when loaitb_id = 21 then DOANHTHU_DONGIA else 0 end) DTPTM_DONGIA_VNPTT
--											, sum(case when loaitb_id = 21 and nguon = 'nvptm' then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_GOI_VNPTT
											, sum(DOANHTHU_DONGIA) DOANHTHU_DONGIA
											
											, sum(case when dichvuvt_id not in (2, 13, 14, 15, 16) then TIEN_THULAO else 0 end) LUONG_DONGIA_CDBR
											, sum(case when loaitb_id = 20 then TIEN_THULAO else 0 end) LUONG_DONGIA_VNPTS
											, sum(case when dichvuvt_id in (13, 14, 15, 16) then TIEN_THULAO else 0 end) LUONG_DONGIA_CNTT
											, sum(case when loaitb_id = 21 and nguon = 'dnhm' then TIEN_THULAO else 0 end) LUONG_DONGIA_DNHM_VNPTT
											, sum(case when loaitb_id = 21 and nguon = 'nvptm' then TIEN_THULAO else 0 end) LUONG_DONGIA_GOI_VNPTT
											, sum(TIEN_THULAO) TIEN_THULAO
--								 select * 
								 from ttkd_bsc.tonghop_ct_dongia_ptm
								 where thang = 202408
								 group by ma_nv, donvi
			;
			update ttkd_bsc.bangluong_dongia_202408 a
				   set (DTPTM_DONGIA_CDBR, DTPTM_DONGIA_CNTT, DTPTM_DONGIA_VNPTS, DTPTM_DONGIA_VNPTT, TONG_DTPTM
							, luong_dongia_cdbr, luong_dongia_cntt, luong_dongia_vnpts
							, luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt) 
						=  (select sum(DTPTM_DONGIA_CDBR), sum(DTPTM_DONGIA_CNTT), sum(DTPTM_DONGIA_VNPTS), sum(DTPTM_DONGIA_VNPTT), sum(DOANHTHU_DONGIA)
											, sum(luong_dongia_cdbr), sum(luong_dongia_cntt), sum(luong_dongia_vnpts)
											, sum(luong_dongia_dnhm_vnptt), sum(luong_dongia_goi_vnptt)
--											, sum(tien_thulao) --4925059739
							 from ttkd_bsc.x_dongia_temp
							 where donvi = 'TTKD' and ma_nv = a.ma_nv
							)
						
					;
			commit;
			rollback;
					
		
		-----TEst trong thang 8, khong chay vi da tinh truc tiep VNPtt trong CT_BSC_PTM
--                           alter table ttkd_bsc.bangluong_dongia_202408 rename column luong_dongia_vnptt to luong_dongia_goi_vnptt;
			update ttkd_bsc.bangluong_dongia_202408 a
			    set (luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt)  =             
								  (select round(sum(nvl(luong_dongia_dnhm, 0)), 0)
												, round(sum(nvl(luong_dongia_goi_kpbdb, 0)) + sum(nvl(luong_dongia_goi_hcm, 0)) + sum(nvl(luong_dongia_goi_qldb, 0)), 0)
											
								   from ttkd_bsc.MANPN_BANGLUONG_DONGIA_VNPTT   ----bangluong_dongia_202408_vnptt 
								   where thang = 202408 and ma_nv = a.ma_nv)
			    where exists (select * from ttkd_bsc.MANPN_BANGLUONG_DONGIA_VNPTT where thang = 202408 and ma_nv = a.ma_nv)
		-----END test
			;
		
			select * from ttkd_bsc.bangluong_dongia_202408 a ;
			
		---Bsung va Thu hoi luong thang 7 do x2 heso
			select ma_nv, (BSUNG+THUHOI)  --549 643 405
				from ttkd_bsc.bangluong_dongia_202407_xuly
				where nvl(CHENHLECH, 0)<>0 and ma_nv not in (select ma_nv from ttkd_bsc.nhanvien where thang = 202408)
					;
		
			update ttkd_bsc.bangluong_dongia_202408 a
					set (luong_dongia_ptm_bs_t7, luong_dongia_ptm_thuhoi_t7) =
							(select BSUNG, THUHOI
								from ttkd_bsc.bangluong_dongia_202407_xuly
								where nvl(CHENHLECH, 0)<>0 and ma_nv = a.ma_nv
							)
					;
					commit;
					
					select sum(BSUNG+THUHOI)  --549 643 405
					from ttkd_bsc.bangluong_dongia_202407_xuly
					where nvl(CHENHLECH, 0)<>0 
					;
					select sum(luong_dongia_ptm_bs_t7+luong_dongia_ptm_thuhoi_t7)  --549 643 405  548492590
					from ttkd_bsc.bangluong_dongia_202408
					--where nvl(CHENHLECH, 0)<>0 
					;


-- Luong don gia ghtt + Dong luc ghtt : Nhu Y
 create table ttkd_bsc.bangluong_dongia_202408_l1 as select * from  ttkd_bsc.bangluong_dongia_202408;
 
			drop table ttkd_bsc.temp_ghtt purge;
			create table ttkd_bsc.temp_ghtt as 
			select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202408
			;
create index ttkd_bsc.temp_ghtt_makpi on ttkd_bsc.temp_ghtt (ma_kpi,loai_tinh,ma_nv);

		---KIEMTRA
				select 'tong' tong, sum(luong_dongia_ghtt) luong_dongia_ghtt, 0 LUONG_DONGIA_GHTT_bsT4
								, sum(giamtru_ghtt_cntt) giamtru_ghtt_cntt, sum(tong_luong_dongia), SUM(thuhoi_dongia_ghtt) thuhoi_dongia_ghtt, sum(tong_luong_thuhoi)
				from ttkd_bsc.bangluong_dongia_202408 a 
				union all
				select 'ghtt_cong', sum(tien) luong_dongia_ghtt, 0,0, 0, 0, 0
					from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202408 and  ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB' ,'DONGIA_TS_TP_TT')
					and  tien <> 0
				union all
				select 'ghtt_cong_bs', sum(tien) luong_dongia_ghtt, 0,0, 0, 0, 0
					from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202408 and  ma_kpi = 'DONGIA' and loai_tinh in ( 'DONGIATRA_OB_BS')
					and  tien <> 0
				union all
				select 'ghtt_tru_cntt', 0, 0, sum(tien) giamtru_ghtt_cntt, 0, 0, 0
						from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202408 and  ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA'
						and  tien <> 0
				union all
				select 'thuhoi_ghtt_tru_ghtt', 0,0, 0, 0, sum(tien) luong_dongia_ghtt, 0
					from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202408 and  ma_kpi = 'DONGIA' and loai_tinh in ('THUHOI_DONGIA_GHTT')
					and  tien <> 0
				;	
			

    UPDATE ttkd_bsc.bangluong_dongia_202408 SET thuhoi_dongia_ghtt = NULL, luong_dongia_ghtt = NULL, giamtru_ghtt_cntt = NULL
		; 
		ROLLBACK;		
		   ;
update ttkd_bsc.bangluong_dongia_202408 a 
    set 
		luong_dongia_ghtt = (select round(sum(tien), 0) 
                        from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB', 'DONGIA_TS_TP_TT')
                                                     and ma_nv = a.ma_nv
                          group by ma_nv having  sum(tien) <> 0) 
		
		, giamtru_ghtt_cntt = (select -sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
												where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_nv = a.ma_nv
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0) 
												
		, thuhoi_dongia_ghtt = (select sum(tien) 
												from ttkd_bsc.tl_giahan_tratruoc 
												where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('THUHOI_DONGIA_GHTT')
																										 and ma_nv = a.ma_nv 
													group by ma_nv having  sum(tien) <> 0) 
	
;
SELECT * FROM ttkd_bsc.bangluong_dongia_202408 a ;
commit;


-- Luong khac: 
			update ttkd_bsc.bangluong_dongia_202408 set luong_khac='', ghichu=' ' 
			    where luong_khac is not null;

 
-- Don gia gia han VNPTS: Chi Nguyen
update ttkd_bsc.bangluong_dongia_202408 a set ghtt_vnpts=''
;
		update ttkd_bsc.bangluong_dongia_202408 a
		    set ghtt_vnpts=(select round(sum(luong_dongia),0) from ttkd_bsc.ghtt_vnpts 
									 where thang=202408 and thang_giao is not null and ma_nv is not null
										    and ma_nv=a.ma_nv)
--		     select ma_nv, ten_vtcv, ten_pb, ghtt_vnpts from ttkd_bsc.bangluong_dongia_202408 a
		    where exists (select 1 from ttkd_bsc.ghtt_vnpts 
								  where thang=202408 and thang_giao is not null and ma_nv is not null and ma_nv=a.ma_nv)
			;

---Don gia cho NV thuc hien Nghiep vu lap ho so, giai quyet khieu nai: a Khanh, chi Hoi
			-----BEGIN Bsung tam 1 thang 5
--		select * from khanhtdt_ttkd.TONGHOP_BSC_KPI_2024 b where b.thang = 202408;
		update ttkd_bsc.bangluong_dongia_202408 Set luong_dongia_nghiepvu = null
		;
			update ttkd_bsc.bangluong_dongia_202408 a
						    Set luong_dongia_nghiepvu = (    select TONGTIEN from khanhtdt_ttkd.TONGHOP_BSC_KPI_2024 b
																			 where b.thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
																				  and b.ma_kpi in ('HCM_SL_CSKHH_004')  ---('HCM_SL_HOTRO_004')					-- <== L?U Ý MÃ KPI
																						 and b.loai_kpi = 'KPI_NV'                             -- <== L?U Ý
																				  and b.ma_nv = a.ma_nv
																		 )
			
			;
		
 commit;
 rollback;

---Don gia cho NV thuc hien xu ly noi dung chung tu: c Nhu, Kim Tien, ap dung ca To Thu ngan hang - NVC
			-----BEGIN Bsung tam 1 thang 6
		
		update ttkd_bsc.bangluong_dongia_202408 Set luong_dongia_chungtu = null;
--		select sum(luong_dongia_chungtu) from ttkd_bsc.bangluong_dongia_202408;
		;
			update ttkd_bsc.bangluong_dongia_202408 a 
						set
								luong_dongia_chungtu =  (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
																				  where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_CHUNG_TU' and ma_nv = a.MA_NV
																				  having  sum(tien)  <> 0
																		)
			where ma_nv not in ('VNP017261', 'VNP017133', 'VNP017603')	----Chi Vuong, Tien thong nhat loai 3 nsvien vi da 100% BSC, khong don gia
					
			;
---Don gia cho NV thuc hien thu cuoc: Cam Lai NVC
			-----BEGIN Bsung tam 1 thang 6
			
--		select sum(TIEN) from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'DONGIA_THUCUOC' and thang = 202408;
		update ttkd_bsc.bangluong_dongia_202408 set luong_dongia_thucuoc = null;
--		select sum(luong_dongia_thucuoc) from ttkd_bsc.bangluong_dongia_202408;
		select sum(thulao_quyettoan)
		from ttkdhcm_ktnv.tyle_thu_bsc_chot a
					left join ttkd_bsc.nhanvien nv on a.thang_thu = nv.thang and a.manv_hrm = nv.ma_nv
		where thang_thu=202408 and loaids_id=2
		;
		
		insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TIEN)
					select a.thang_thu, 'DONGIA_THUCUOC' LOAI_TINH, 'DONGIA' MA_KPI, nv.MA_NV, nv.MA_TO, nv.MA_PB, round(a.thulao_quyettoan, 0) tien 
					from ttkdhcm_ktnv.tyle_thu_bsc_chot a
									join ttkd_bsc.nhanvien nv on nv.donvi = 'TTKD' and a.thang_thu = nv.thang and a.manv_hrm = nv.ma_nv
					where thang_thu=202408 and loaids_id=2
		;
		delete from ttkd_bsc.tl_giahan_tratruoc where thang is null;
		
			update ttkd_bsc.bangluong_dongia_202408 a 
						set
								luong_dongia_thucuoc =  (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
																				  where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_THUCUOC' and ma_nv = a.MA_NV
																				  having  sum(tien)  <> 0)
			;		
		
 commit; 
		
		----*****LUONG DON GIA THAU PGP theo file GGS******----
		update ttkd_bsc.bangluong_dongia_202408 a 
						set luong_khac = 16687800
--		select * from ttkd_bsc.bangluong_dongia_202408 a 
		where ma_nv in ('VNP027259', 'VNP017190')
		;
		------END THAU----
-- Tong luong don gia:  chay dot 1, dot 2
--		update ttkd_bsc.bangluong_dongia_202408 set tong_luong_dongia='' where tong_luong_dongia is not null
		;
	
		update ttkd_bsc.bangluong_dongia_202408
		    set tong_luong_dongia = round(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
																    + nvl(luong_dongia_dnhm_vnptt,0) + nvl(luong_dongia_goi_vnptt,0)
																    + nvl(luong_dongia_ghtt,0)
																    + nvl(luong_dongia_ptm_bs_t7, 0)
																    + nvl(luong_khac,0)+nvl(ghtt_vnpts,0) 
																    + nvl(luong_dongia_nghiepvu, 0)
																    + nvl(luong_dongia_chungtu, 0)
																    + nvl(luong_dongia_thucuoc, 0)
																, 0)
																
																
		;
		
		
		update ttkd_bsc.bangluong_dongia_202408 a
					   set tong_thulao_thucchi = nvl(tong_luong_dongia, 0) - nvl(tong_luong_thuhoi, 0)
			
			;
			update ttkd_bsc.bangluong_dongia_202408 a
			   set tong_thulao_thucchi='' 
			   where tong_thulao_thucchi=0 
		    ;
     --    select * from ttkd_bsc.bangluong_dongia_202408 where  ma_nv in ('VNP016902',
'VNP017072',
'VNP017853',
'VNP017014',
'VNP016898',
'VNP019529');

commit;

---da xong
    
    
/* -- Kiem tra tong luong don gia: 
			TONG_LUONG_DONGIA=LUONG_DONGIA_CDBR+LUONG_DONGIA_CNTT+LUONG_DONGIA_VNPTS+CTVXHH_QLY_PTR_CTV+LUONG_DONGIA_DNHM_VNPTT+LUONG_DONGIA_GOI_KPBDB+LUONG_DONGIA_GOI_HCM+LUONG_DONGIA_GOI_QLDB+LUONG_DONGIA_GHTT+DONGLUC_GHTT+DONGLUC_GHTS+GHTT_VNPTS+LUONG_KHAC
			
			select sum(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
							+nvl(luong_dongia_vnptt,0)+nvl(ctvxhh_qly_ptr_ctv,0) 
							+nvl(luong_dongia_ghtt,0)+nvl(dongluc_ghtt,0)+nvl(dongluc_ghts,0)
							+nvl(luong_khac,0)+nvl(ghtt_vnpts,0)
						 ) tong
			    from  ttkd_bsc.bangluong_dongia_202408
			union all
			select sum(tong_luong_dongia) from ttkd_bsc.bangluong_dongia_202408;

*/

select sum(tong_thulao_thucchi) from ttkd_bsc.bangluong_dongia_202408;--5233772880  --5770363499
            
-- THUHOI: 
			update ttkd_bsc.bangluong_dongia_202408 a
						 set  
								 giamtru_hosotainha=''--, giamtru_ghtt_cntt=''
								, luong_dongia_ptm_thuhoi=''--, thuhoi_dongia_ghtt = null
								, luong_dongia_khac_thuhoi=''
								, tong_luong_thuhoi = null
				;
			select *  from ttkd_bsc.bangluong_dongia_202408
			;
			select loaitb_id, sum(tien_thuhoi) from ttkd_bsc.ct_thuhoi where thang=202408 group by loaitb_id;
			;
--- thu hoi PTM		
				update ttkd_bsc.bangluong_dongia_202408 a  
				   set luong_dongia_ptm_thuhoi = (select sum(tien_thuhoi) from ttkd_bsc.ct_thuhoi where thang=202408 and ma_nv=a.ma_nv)
				   where exists(select 1 from ttkd_bsc.ct_thuhoi where thang=202408 and ma_nv=a.ma_nv)
				   ;
-- giam tru do thu ho so tai nha: 		
			update ttkd_bsc.bangluong_dongia_202408 a
				   set giamtru_hosotainha=
					   (select round(sum(tien_giam) ,0) from ttkd_bsc.ct_giamtru where thang=202408 and ma_nv=a.ma_nv)
			    --  select giamtru_hosotainha from ttkd_bsc.bangluong_dongia_202408  a
			    where exists (select 1 from ttkd_bsc.ct_giamtru where thang=202408 and ma_nv=a.ma_nv)
			    ;


-- tong_luong_thuhoi:
update ttkd_bsc.bangluong_dongia_202408 a set tong_luong_thuhoi='' where tong_luong_thuhoi is not null
;
			update ttkd_bsc.bangluong_dongia_202408 a
			   set tong_luong_thuhoi = round( nvl(luong_dongia_ptm_thuhoi,0) +  nvl(luong_dongia_khac_thuhoi,0)
																	+ nvl(giamtru_hosotainha,0) 
																	+ nvl(giamtru_ghtt_cntt,0)  + nvl(thuhoi_dongia_ghtt, 0)
																	+ nvl(luong_dongia_ptm_thuhoi_t7, 0) 
															, 0)
															;
			
			
			update ttkd_bsc.bangluong_dongia_202408 a
					   set tong_thulao_thucchi = nvl(tong_luong_dongia, 0) - nvl(tong_luong_thuhoi, 0)
			
			;
			update ttkd_bsc.bangluong_dongia_202408 a
			   set tong_thulao_thucchi='' 
			   where tong_thulao_thucchi=0
			   
			   ;
			   
			commit;


			/*
			-- Kiem tra thu hoi:
			select '4 field tien thu hoi' field, 
					  round( sum (nvl(luong_dongia_ptm_thuhoi,0)  +  nvl(luong_dongia_khac_thuhoi,0)
					  +  nvl(giamtru_hosotainha,0)   +  nvl(giamtru_phathuy_qldb,0) 
					   ) ,0)      tien_thuhoi
			from ttkd_bsc.bangluong_dongia_202408
			union all
			select 'tien_luong_thuhoi', sum(tong_luong_thuhoi) from ttkd_bsc.bangluong_dongia_202408;
			*/


-- XUAT GUI BANG LUONG DON GIA: gui NSU
	select sum(tong_luong_dongia) - sum(luong_dongia_ghtt_bst4) tong_luong_dongia_l1, sum(tong_luong_dongia) tong_luong_dongia_l2, sum(tong_luong_thuhoi) tong_luong_thuhoi, sum(luong_dongia_ghtt_bst4) luong_dongia_ghtt_bst4 from ttkd_bsc.bangluong_dongia_202404; 
     	select to_char(sum(tong_luong_dongia), '999g999g999g999') tong_luong_dongia, to_char(sum(tong_luong_thuhoi), '999g999g999') tong_luong_thuhoi, to_char(sum(TONG_THULAO_THUCCHI), '999g999g999g999') TONG_THULAO_THUCCHI
		from ttkd_bsc.bangluong_dongia_202408
		where donvi = 'TTKD' and TONG_THULAO_THUCCHI is not null --nvl(tong_luong_dongia, 0) + nvl(tong_luong_thuhoi, 0) > 0
						and ghichu is  null; 
	 
	   
	   -- 2 099 414 559 16g35 4/7/24
	   
	   --5 967 854 054	274 841 178  20/07/24 14g39
	   --6,016,308,054  274,841,178		24/07/24 15g38
	   --    6,101,743,404	 274,841,178 24/07/24 16g21		---chi VUong 12 nv nvc 
	   --    6,105,601,404	 274,841,178 24/07/24 23g08
	   --    6,093,867,404	 274,841,178 25/07/24 13g08		---chi Nhu, sep Thao thong nhat 8 nvc
	   
	   --    2,425,446,332 16g26 4/8/24
	   --    6,029,242,935	 338,400,092 11g51 16/08/24
	   --   6,287,132,163	 334,739,253  17g50 20/08/2024
	   --    6,159,878,169	 334,871,253  17g50 20/08/2024
	   --    6,160,326,765	 291,755,141
	   --    6,160,326,765	 335,001,333 18g50 20/08/2024
	   --    6,160,279,964	 333,529,733 22g20 20/08/2024
	   --    6,130,208,101	 333,529,733 16g40 22/08/2024
	   --    6,136,311,839	 333,529,733 17g40 22/08/2024
	   --    6,229,761,416	 333,529,733
	   
	   --   2,546,646,145	 134,571,803 16g40 04/09/2024
	   --    2,304,818,712	 134,571,803 19g40 04/09/2024
	   --       5,756,371,519	 506,673,220	   5,249,698,299  14g 16/9/2024
--				   5,758,743,647	 555,544,185	   5,203,199,462 (bsung them not exits nv)
	--    6,277,036,719	 487,095,235	   5,789,941,484  20/09/2024
--	      6,279,406,301	 535,966,200	   5,743,440,101  (bsung them not exits nv)

	----NSu tren 100tr gui NSU
		select ma_nv, ten_nv, ten_vtcv, ten_pb, tong_luong_dongia from ttkd_bsc.bangluong_dongia_202408 where tong_luong_dongia>40000000
		;
		
		select * from ttkd_bsc.ct_bsc_ptm where thang_tldg_dt=202408 and manv_ptm='VNP031192'; and loaitb_id = 20;
		VNP051627
VNP031192
	---gui chi tiet NSU
		select 
				ma_nv, ten_nv,ma_vtcv,ten_vtcv,	ma_pb,	ten_pb,	ma_to,ten_to,	loai_ld
				 , dtptm_dongia_cdbr,	dtptm_dongia_vnpts,	dtptm_dongia_cntt, tong_dtptm
				  , luong_dongia_cdbr, luong_dongia_cntt, luong_dongia_vnpts, 
				  luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt, ghtt_vnpts,	luong_khac,
				  luong_dongia_ghtt, luong_dongia_nghiepvu, luong_dongia_chungtu, luong_dongia_thucuoc
				 , LUONG_DONGIA_PTM_BS_T7, tong_luong_dongia,	ghichu	,
				  luong_dongia_ptm_thuhoi,	luong_dongia_khac_thuhoi, thuhoi_dongia_ghtt
				  , giamtru_hosotainha, giamtru_ghtt_cntt, LUONG_DONGIA_PTM_THUHOI_T7, tong_luong_thuhoi
				  , TONG_THULAO_THUCCHI
		from ttkd_bsc.bangluong_dongia_202408
		where donvi = 'TTKD'; and TONG_THULAO_THUCCHI is not null--nvl(tong_luong_dongia, 0) + nvl(tong_luong_thuhoi, 0) > 0
		;

select * from ttkd_bsc.bangluong_dongia_202408;
select * from ttkd_bsc.bangluong_kpi_202408 ;
select MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG, TYTRONG, GIAO, THUCHIEN, TYLE_THUCHIEN, MUCDO_HOANTHANH, NGAY_PUBLIC
from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_DT_PTMOI_021' ;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202408 and loai_tinh = 'DONGIATRA_OB';

select *--MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO
--			, HCM_DT_PTMOI_021
--			, HCM_DT_PTMOI_053
--			, HCM_DT_PTMOI_053_giao
--			, HCM_CL_DHQLY_006
--			, HCM_CL_DHQLY_007
--			, HCM_DT_PTMOI_056
--			, HCM_SL_COMBO_006
from ttkd_bsc.bangluong_dongia_202408; where ghichu = 'khongtontai';--ma_nv = 'VNP016902';

SELECT ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb,ma_to, ten_to,ma_kpi, ten_kpi, to_char(giatri) giatri
  FROM ttkd_bsc.bangluong_kpi_202408_tracuu where ma_nv = 'VNP020231';
  ;
delete from ttkd_bsc.bangluong_dongia_202408  where ghichu = 'khongtontai';
			----Gui NSU nvien khong ton tai trong bang nhanvien
		insert into ttkd_bsc.bangluong_dongia_202408 
			(donvi, MA_NV, ten_nv, ten_to, ten_pb, ghichu, luong_dongia_cdbr, luong_dongia_vnpts, luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt, luong_dongia_cntt, tong_luong_dongia, tong_luong_thuhoi, tong_thulao_thucchi)
			 
			(select nv.donvi, a.ma_nv, nv.ten_nv, nv.ten_to, nv.ten_pb
					, 'khongtontai' ghichu
				    , sum(luong_dongia_cdbr) luong_dongia_cdbr
				    , sum(luong_dongia_vnpts) luong_dongia_vnpts
				    , sum(luong_dongia_dnhm_vnptt) luong_dongia_dnhm_vnptt
				    , sum(luong_dongia_goi_vnptt) luong_dongia_goi_vnptt
				    , sum(luong_dongia_cntt) luong_dongia_cntt
				    , sum(tien_thulao) + sum(ghtt_vnpts) tong_luong_dongia
				    , sum(tien_thuhoi) tong_luong_thuhoi
				    , sum(tien_thulao) + sum(ghtt_vnpts) - sum(tien_thuhoi) tong_thulao_thucchi
			from (--x_dongia_temp
					select ma_nv, luong_dongia_cdbr, luong_dongia_vnpts, luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt, luong_dongia_cntt, tien_thulao, 0 ghtt_vnpts, 0 tien_thuhoi
					  from ttkd_bsc.x_dongia_temp a 
					  where not exists(select 1 from ttkd_bsc.nhanvien where thang = 202408 and ma_nv = a.ma_nv)			--thang n
								 and exists (select 1 from ttkd_bsc.nhanvien where thang < 202408 and donvi = 'TTKD' and ma_nv = a.ma_nv)	--thang n-1
								 and tien_thulao > 0 
							
					union all
							select ma_nv, 0, 0, 0, 0, 0, 0, round(sum(luong_dongia),0), 0
							from ttkd_bsc.ghtt_vnpts a
							where a.thang=202408 and thang_giao is not null and ma_nv is not null
										    and not exists (select 1 from ttkd_bsc.nhanvien where thang = a.thang and ma_nv = a.ma_nv)		
											and exists (select 1 from ttkd_bsc.nhanvien where thang < 202408 and donvi = 'TTKD'  and ma_nv = a.ma_nv)
							group by ma_nv
					union all
							select ma_nv, 0, 0, 0, 0, 0, 0, 0, sum(tien_thuhoi)
							from ttkd_bsc.ct_thuhoi a
							where a.thang=202408
											and not exists (select 1 from ttkd_bsc.nhanvien where thang = a.thang and ma_nv = a.ma_nv)		
											and exists (select 1 from ttkd_bsc.nhanvien where thang < 202408 and donvi = 'TTKD'  and ma_nv = a.ma_nv)
							group by a.ma_nv
						) a
					left join (select donvi, ma_nv, ten_nv, ten_to, ten_pb, row_number() over (partition by ma_nv order by thang desc) rnk
										from ttkd_bsc.nhanvien
										where thang <= 202408) nv on a.ma_nv = nv.ma_nv and rnk = 1
			group by a.ma_nv, nv.donvi, nv.ma_nv, nv.ten_nv, nv.ten_to, nv.ten_pb
			)
			  ;
			  commit;


---Anh Nghia tren 100tr/thang
select manv_ptm, sum(dg_goi) tong_dongia
from (
		select manv_ptm, luong_dongia_nvptm dg_goi from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202408 and  manv_ptm is not null
		union all
		select manv_ptm, luong_dongia_dnhm_nvptm dg_dnhm from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202408 and manv_ptm is not null
)
group by manv_ptm
having sum(dg_goi)>100000000
-- chi tiet gui anh Nghia:
			select id, thang_ptm,ma_gd, tenkieu_ld,ma_tb,dich_vu,loaitb_id, ten_tb,diachi_ld--,sdt_lh, email_lh
				,sothang_dc,ma_da,
				   to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg, to_char(ngay_luuhs_ttkd,'dd/mm/yyyy') ngay_luuhs_ttkd, to_char(ngay_luuhs_ttvt,'dd/mm/yyyy') ngay_luuhs_ttvt,
				   to_char(ngay_scan,'dd/mm/yyyy') ngay_scan, goi_cuoc
				  ,ma_nguoigt,nguoi_gt,manv_tt_dai,ma_to_dai,manv_hotro,tyle_hotro
				  , manv_ptm ma_nv,ten_pb,ma_to,ten_to,ma_pb, ghi_chu,lydo_khongtinh_luong
				   ,tien_dnhm,tien_sodep,ngay_tt,tien_tt, trangthai_tt_id
				   dthu_ps_truoc,dthu_ps,dthu_ps_n1,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,chiphi_ttkd,
				   doituong_kh,khhh_khm,diaban,phanloai_kh,heso_khachhang,heso_dichvu,heso_dichvu_1,heso_tratruoc,heso_khuyenkhich,
				   heso_tbnganhan,heso_kvdacthu,heso_vtcv_nvptm,
				   heso_vtcv_dai,heso_vtcv_nvhotro,heso_hotro_nvptm,heso_hotro_dai,heso_hotro_nvhotro,
				   heso_quydinh_nvptm,heso_quydinh_dai,heso_quydinh_nvhotro,heso_diaban_tinhkhac,tyle_huongdt, heso_hoso, heso_daily
				   doanhthu_dongia_nvptm,doanhthu_dongia_dai,doanhthu_dongia_nvhotro,
				   heso_dichvu_dnhm,doanhthu_dongia_dnhm,doanhthu_kpi_nvptm,
				   thang_tldg_dnhm,thang_tldg_dt,thang_tlkpi,thang_tlkpi_to,lydo_khongtinh_dongia,
				   luong_dongia_dnhm_nvptm,luong_dongia_nvptm,luong_dongia_dai,luong_dongia_nvhotro, thang_tldg_dt_nvhotro,
				   doanhthu_kpi_dnhm, thang_tlkpi_dnhm
 from ttkd_bsc.ct_bsc_ptm 
where( (thang_ptm>=202405 and (thang_tldg_dt=202408 or thang_tldg_dt is null)) or (thang_ptm<202405 and thang_tldg_dt=202408)
			    )
						and manv_ptm in ('CTV021976',
'CTV062104',
'CTV073755',
'VNP017861',
'VNP031192',
'VNP051627')
order by thang_tldg_dt 
			    ; 

			
			select * from ttkd_bsc.ct_bsc_ptm_pgp 
			    where (thang_ptm>=202402 and (thang_tldg_dt_nvhotro=202408 or thang_tldg_dt_nvhotro is null)) 
					  or (thang_ptm<202402 and (thang_tlkpi_hotro=202408 or thang_tldg_dt_nvhotro=202408)  )
					  ; 


-- Kiem tra dnhm:
select manv_ptm,dich_vu,tien_dnhm, a.HESO_DICHVU_DNHM,DOANHTHU_DONGIA_DNHM, DOANHTHU_KPI_DNHM, DOANHTHU_KPI_DNHM_PHONG,
            thang_tldg_dnhm, thang_tlkpi_dnhm, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG
		  , lydo_khongtinh_luong, lydo_khongtinh_dongia
from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202408 and dichvuvt_id not in (2,13,14,15,16) and tien_dnhm>0 and ngay_tt is not null and thang_tldg_dnhm is null;

select * from ttkd_bsc.bangluong_dongia_202408;


-- GUI PNS : PTM
select a.ten_pb, a.ten_to, a.ma_nv,  a.ten_nv, a.ten_vtcv
            ,b.tong_dtptm tong_dtptm_old, b.heso_qd_tong heso_qd_tong_old
            ,a.tong_dtptm tong_dtptm_new, a.heso_qd_tong heso_qd_tong_new
            ,a.tong_luong_dongia tong_luong_dongia_new, b.tong_luong_dongia tong_luong_dongia_old                     
           ,nvl(b.luong_dongia_cdbr,0)+nvl(b.luong_dongia_vnpts,0)+nvl(b.luong_dongia_cntt,0) +nvl(b.luong_dongia_vnptt,0) tongluong_ptm_old
            ,nvl(a.luong_dongia_cdbr,0)+nvl(a.luong_dongia_vnpts,0)+nvl(a.luong_dongia_cntt,0)+nvl(a.luong_dongia_vnptt,0) tongluong_ptm_new                         
             ,nvl(a.tong_luong_dongia,0) - nvl(b.tong_luong_dongia,0) chechlech
		   , b.luong_dongia_ghtt  luong_dongia_ghtt_old, a.luong_dongia_ghtt luong_dongia_ghtt_new
from ttkd_bsc.bangluong_dongia_202408 a, ttkd_bsc.bangluong_dongia_202408_l3 b 
where a.ma_nv=b.ma_nv and nvl(a.tong_luong_dongia,0)<>nvl(b.tong_luong_dongia,0)       
order by (a.tong_luong_dongia - b.tong_luong_dongia);


select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ten_vtcv
             ,a.tong_luong_thuhoi tong_luong_thuhoi_new, b.tong_luong_thuhoi tong_luong_thuhoi_old,
             nvl(a.tong_luong_thuhoi,0) - nvl(b.tong_luong_thuhoi,0) chechlech

  from ttkd_bsc.bangluong_dongia_202408 a, ttkd_bsc.bangluong_dongia_202408_l3 b 
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
			    update  ttkd_bsc.ct_bsc_ptm set thang_luong = 9999, THANG_TLDG_DT = 202408, THANG_TLKPI = 202408, THANG_TLKPI_TO = 202408, THANG_TLKPI_PHONG = 202408--, heso_dichvu = 0.3
--					select * from 	ttkd_bsc.ct_bsc_ptm
						where ma_gd = 'HCM-LD/01579295' and manv_ptm in ('VNP030420') --and ma_tb in ('hcm_colo_00010720', 'hcm_colo_00010838', 'hcm_colo_00010836')
			    ;
			    commit;
			    rollback;
   


