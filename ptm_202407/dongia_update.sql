	create table ttkd_bsc.bangluong_dongia_202407 (
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
				
				dtptm_quydinh               number,
				dtptm_muctieu               number,
				dtptm_bq3t                     number,
				heso_qd_tong                 number,
				
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
				luong_khac                       number,
				tong_luong_dongia         number,
				tinh_bsc number
				)
				;
				insert into ttkd_bsc.bangluong_dongia_202407
								 (donvi, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld, nhomld_id)
							SELECT donvi, ma_nv, ten_nv, ma_vtcv, ten_vtcv, ma_pb, ten_pb, ma_to, ten_to, loai_ld, nhomld_id
							  FROM ttkd_bsc.nhanvien
							  where thang = 202407
			  ; 
		commit;
		
-- he so quy doi cua tong dthu:     
			---Ap dung vb 323 dv BHKV, 292 BHDN
			update x_bangluong_temp a 
						set heso_dthu = null
						;
			select thang, ma_nv from bang_heso_dthu group by thang, ma_nv having count(*)>1;
			create table ttkd_bsc.bang_heso_dthu as select * from bang_heso_dthu;
			insert into ttkd_bsc.bang_heso_dthu as 
						select thang, DONVI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_PB, TEN_PB, MA_TO, TEN_TO, LOAI_LD
							from ttkd_bsc.nhanvien where thang = 202407 and donvi = 'TTKD'
						;
			----Update Heso dthu tu 2 table ttkd_bsc.dinhmuc_giao_ptm va TLTH chi tieu 1 KHDN
					update ttkd_bsc.bang_heso_dthu
						set heso_dthu = 
					where thang = 202407
					;
			----END
			delete from ttkd_bsc.bang_heso_dthu where thang = 202407;
			commit;
		
			---Nvien moi danh gia bsc --> heso_dthu null OR < 1 --> assign = 1, nguoc lai giu nguyen 
			update ttkd_bsc.bang_heso_dthu a set heso_dthu = 1
--			select * from x_bangluong_temp a
			where exists (select * from ttkd_bsc.nhanvien where thang = 202407 and tinh_bsc = 0 and ma_nv = a.ma_nv)
						and nvl(heso_dthu, 0) <1
			;
			commit;
			rollback;
		
		
		select * from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202407;

drop table ttkd_bsc.tonghop_ct_dongia_ptm purge;
select sum(TIEN_THULAO) from ttkd_bsc.tonghop_ct_dongia_ptm; where ma_nv = 'VNP019958'; thang 7 = 6539341849
create table ttkd_bsc.tonghop_ct_dongia_ptm as
			with nv as (select thang, donvi, ma_nv, ten_nv, ten_to, ten_pb, ma_to, ma_pb, ma_vtcv, nhomld_id
									from ttkd_bsc.nhanvien where thang = 202407
									union
									select 202407 thang, 'POT' donvi, MA_NV, TEN_NV, TEN_dv, null, 'POT', null, null, 5 nhomld_id 
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
										when loaitb_id = 21 and nv.donvi in ('VTTP', 'POT') then 1				---VTTP, POTMASCO VNPtt heso = 1
										when nv.donvi in ('VTTP', 'POT') and thang_ptm < 202407 then 1 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										when nv.donvi in ('VTTP', 'POT') and thang_ptm >= 202407 then 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										else nvl(hs.heso_dthu, 1) end heso_dthu
							, round(case when loaitb_id = 21 and nguon = 'dnhm' and nv.ma_vtcv in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_17') then luong_dongia
													when loaitb_id = 21 and nv.donvi in ('VTTP', 'POT') then luong_dongia
													when loaitb_id = 21 and nguon = 'nvptm' then luong_dongia * nvl(hs.heso_dthu, 1)
													when nv.donvi in ('VTTP', 'POT') and thang_ptm < 202407 then luong_dongia 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
													when nv.donvi in ('VTTP', 'POT') and thang_ptm >= 202407 then luong_dongia * 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
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
					    where manv_ptm is not null and thang_tldg_dt = 202407
					    
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
					    where manv_ptm is not null and thang_tldg_dnhm = 202407 
					   
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
									  , case when loaitb_id = 21 then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_vnptt
									  , case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_khac
									  , luong_dongia_dnhm_nvptm * nvl(heso_hotro_nvhotro, 1) luong_dongia
									 , 'dnhm' nguon, thang_tldg_dnhm, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_hotro is not null and thang_tldg_dnhm = 202407 
										and tyle_hotro is null and tyle_am is null and nvl(vanban_id, 0) != 764 ---only T7 xoa
					    
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
					    where manv_tt_dai is not null and thang_tldg_dt_dai = 202407
		
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
					    where manv_hotro is not null and thang_tldg_dt_nvhotro = 202407 
									and not exists (select 1 from ttkd_bsc.nhanvien where thang = 202407 and ma_pb = 'VNP0702600' and ma_nv = a.manv_hotro)
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
					    where manv_hotro is not null and thang_tldg_dt_nvhotro = 202407
			    ) a
			    
			left join nv on nv.thang = a.thang_dongia and nv.ma_nv = a.manv_ptm
			left join bang_heso_dthu hs on hs.thang = 202407 and a.manv_ptm = hs.ma_nv
			where manv_ptm is not null 
			; 
		

			
	----all dvu ngoai tru VNPtt
			select * from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202407;
			select * from ttkd_bsc.bangluong_dongia_202407 a;
			update ttkd_bsc.bangluong_dongia_202407 
						set TONG_LUONG_DONGIA = null, heso_qd_tong = null, luong_dongia_cdbr = null
									, luong_dongia_vnpts = null, luong_dongia_cntt = null, luong_dongia_dnhm_vnptt = null, luong_dongia_goi_vnptt = null
						;
			select sum(tong_luong_dongia * nvl(heso_dthu, 1)), ma_nv
						 from ttkd_bsc.tonghop_ct_dongia_ptm
						 group by ma_nv
			;
			drop table ttkd_bsc.x_dongia_temp purge;
			create table ttkd_bsc.x_dongia_temp as
					select ma_nv, donvi
											, sum(case when dichvuvt_id not in (2, 13, 14, 15, 16) then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_CDBR
											, sum(case when loaitb_id = 20 then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_VNPTS
											, sum(case when dichvuvt_id in (13, 14, 15, 16) then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_CNTT
											, sum(case when loaitb_id = 21 and nguon = 'dnhm' then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_DNHM_VNPTT
											, sum(case when loaitb_id = 21 and nguon = 'nvptm' then DOANHTHU_DONGIA else 0 end) DOANHTHU_DONGIA_GOI_VNPTT
											, sum(DOANHTHU_DONGIA) DOANHTHU_DONGIA
											
											, sum(case when dichvuvt_id not in (2, 13, 14, 15, 16) then TIEN_THULAO else 0 end) LUONG_DONGIA_CDBR
											, sum(case when loaitb_id = 20 then TIEN_THULAO else 0 end) LUONG_DONGIA_VNPTS
											, sum(case when dichvuvt_id in (13, 14, 15, 16) then TIEN_THULAO else 0 end) LUONG_DONGIA_CNTT
											, sum(case when loaitb_id = 21 and nguon = 'dnhm' then TIEN_THULAO else 0 end) LUONG_DONGIA_DNHM_VNPTT
											, sum(case when loaitb_id = 21 and nguon = 'nvptm' then TIEN_THULAO else 0 end) LUONG_DONGIA_GOI_VNPTT
											, sum(TIEN_THULAO) TIEN_THULAO
--								 select * 
								 from ttkd_bsc.tonghop_ct_dongia_ptm
								 where thang = 202407
								 group by ma_nv, donvi
			;
			update ttkd_bsc.bangluong_dongia_202407 a
				   set (luong_dongia_cdbr, luong_dongia_cntt, luong_dongia_vnpts
							, luong_dongia_dnhm_vnptt, luong_dongia_goi_vnptt) 
						=  (select sum(luong_dongia_cdbr), sum(luong_dongia_cntt), sum(luong_dongia_vnpts)
											, sum(luong_dongia_dnhm_vnptt), sum(luong_dongia_goi_vnptt)
	--										, sum(tien_thulao)
							 from ttkd_bsc.x_dongia_temp
							 where donvi = 'TTKD' and ma_nv = a.ma_nv
							)
					;
		
	
	----test----cach tinh moi thang 7
	
			update x_bangluong_dongia_202407
					set TONG_LUONG_DONGIA = round(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
																				    + nvl(luong_dongia_dnhm_vnptt,0) + nvl(luong_dongia_goi_vnptt,0)
																				, 0)
			;
			drop table x_bangluong_dongia_202407_goc purge;
			select * from x_bangluong_dongia_202407_goc where ma_nv = 'VNP017385';
			create table x_bangluong_dongia_202407_goc as
				select ma_nv,  LUONG_DONGIA_CDBR, LUONG_DONGIA_CNTT, LUONG_DONGIA_VNPTS, LUONG_DONGIA_DNHM_VNPTT, LUONG_DONGIA_GOI_VNPTT, HESO_QD_TONG
								, round(nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)
																				    + nvl(luong_dongia_dnhm_vnptt,0) + nvl(luong_dongia_goi_vnptt,0)
																				, 0) TONG_LUONG_DONGIA
				from ttkd_bsc.bangluong_dongia_202407
			;
select a.MA_NV, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_PB, a.TEN_PB, a.MA_TO, a.TEN_TO, a.TONG_LUONG_DONGIA
--				, nvl(a.TONG_LUONG_DONGIA, 0) - nvl(a.TONG_LUONG_THUHOI, 0) as tongchi_t7
--			, nvl(b.TONG_LUONG_DONGIA, 0) - nvl(b.TONG_LUONG_THUHOI, 0) as dachi_t7
			, case when a.tong_luong_dongia - b.tong_luong_dongia < 0 then to_char(a.tong_luong_dongia - b.tong_luong_dongia, '999g999g999g999') else null end thuhoi
			, case when a.tong_luong_dongia - b.tong_luong_dongia > 0 then to_char(a.tong_luong_dongia - b.tong_luong_dongia, '999g999g999g999') else null end bsung
			, a.tong_luong_dongia - b.tong_luong_dongia chenhlech
from x_bangluong_dongia_202407 a
			join x_bangluong_dongia_202407_goc b on a.ma_nv = b.ma_nv
where 
--				a.HESO_dthu <> b.HESO_QD_TONG
				a.TONG_LUONG_DONGIA <> b.TONG_LUONG_DONGIA