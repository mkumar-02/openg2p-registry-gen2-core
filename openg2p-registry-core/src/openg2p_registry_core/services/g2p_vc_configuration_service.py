import logging
import uuid
from typing import List, Optional

from openg2p_fastapi_common.service import BaseService
from openg2p_fastapi_common.context import dbengine

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy import select

from ..models import (
    DataModel,
    G2PIntakeFormDefinition,
    G2PRegisterDefinition,
    G2PRegistryVcConfiguration,
)
from ..schemas import (
    VcConfigurationData,
)
from ..errors import G2PRegistryErrorCodes, G2PRegistryException

_logger = logging.getLogger("g2p-outgestion-configuration-service")

class G2PVcConfigurationService(BaseService):

    async def get_vc_configuration_for_register(
        self,
        register_id: str
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            await self._validate_register_exists(register_id, session)
            _logger.info("validation Register exists")
            vc_configuration_data = await self._fetch_vc_configuration_data_list(
                session, register_id=register_id
            )
            _logger.info(
                f"Got {len(vc_configuration_data)} vc configurations for register id {register_id}"
            )
            return vc_configuration_data

    async def get_all_vc_configurations(
        self
    ) -> List[VcConfigurationData]:
        """Get all registry vc configurations"""
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            vc_configuration_data = await self._fetch_vc_configuration_data_list(session)
            _logger.info(f"Got {len(vc_configuration_data)} vc configurations")
            return vc_configuration_data

    async def create_vc_configuration(
        self, 
        register_id: str,
        vc_mnemonic: str,
        descriptor_schema: str
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            await self._validate_register_exists(register_id, session)
            _logger.info("validation Register exists")

            g2p_register_vc_configuration = G2PRegistryVcConfiguration(
                vc_config_id=str(uuid.uuid4()),
                register_id=register_id,
                vc_mnemonic=vc_mnemonic,
                descriptor_schema=descriptor_schema
            )
            session.add(g2p_register_vc_configuration)
            await session.commit()
    
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]

    async def edit_descriptor_schema(
        self, 
        vc_config_id: str,
        descriptor_schema: dict
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            g2p_register_vc_configuration: G2PRegistryVcConfiguration = await self._get_vc_configuration(vc_config_id, session)

            g2p_register_vc_configuration.descriptor_schema = descriptor_schema
            
            await session.commit()
            await session.refresh(g2p_register_vc_configuration)
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]

    async def remove_vc_configuration(
        self, 
        vc_config_id: str
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            g2p_register_vc_configuration = await self._get_vc_configuration(vc_config_id, session)

            await session.delete(g2p_register_vc_configuration)
            await session.commit()
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]


    async def _fetch_vc_configuration_data_list(
        self,
        session: AsyncSession,
        register_id: Optional[str] = None,
    ) -> List[VcConfigurationData]:
        stmt = (
            select(
                G2PRegistryVcConfiguration,
                G2PIntakeFormDefinition.form_mnemonic,
                DataModel.data_model_mnemonic,
            )
            .outerjoin(
                G2PIntakeFormDefinition,
                G2PRegistryVcConfiguration.intake_form_id
                == G2PIntakeFormDefinition.form_id,
            )
            .outerjoin(
                DataModel,
                G2PRegistryVcConfiguration.data_model_id == DataModel.data_model_id,
            )
        )
        if register_id is not None:
            stmt = stmt.where(G2PRegistryVcConfiguration.register_id == register_id)

        rows = (await session.execute(stmt)).all()
        return [
            VcConfigurationData(
                vc_config_id=vc.vc_config_id,
                register_id=vc.register_id,
                intake_form_id=vc.intake_form_id,
                intake_form_mnemonic=intake_form_mnemonic,
                data_model_id=vc.data_model_id,
                data_model_mnemonic=data_model_mnemonic,
                vc_mnemonic=vc.vc_mnemonic,
                descriptor_schema=vc.descriptor_schema,
            )
            for vc, intake_form_mnemonic, data_model_mnemonic in rows
        ]

    async def _validate_register_exists(self, register_id: str, session: AsyncSession):
        """Validate that a register exists."""
        register_definition = await session.get(G2PRegisterDefinition, register_id)

        if not register_definition:
            raise G2PRegistryException(
                code=G2PRegistryErrorCodes.REGISTER_NOT_FOUND.value[1],
                message=f"Register with id {register_id} not found"
            )
    
    async def _get_vc_configuration(self, vc_config_id: str, session: AsyncSession) -> G2PRegistryVcConfiguration:
        g2p_register_vc_configuration = await session.get(G2PRegistryVcConfiguration, vc_config_id)

        if not g2p_register_vc_configuration:
            raise G2PRegistryException(
                code=G2PRegistryErrorCodes.VC_CONFIGURATION_NOT_FOUND.value[1],
                message=f"VC configuration with id {vc_config_id} not found"
            )
        return g2p_register_vc_configuration