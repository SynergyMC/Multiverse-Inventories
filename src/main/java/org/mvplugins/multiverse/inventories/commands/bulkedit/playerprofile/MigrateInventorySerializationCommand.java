package org.mvplugins.multiverse.inventories.commands.bulkedit.playerprofile;

import com.google.common.io.Files;
import org.jvnet.hk2.annotations.Service;
import org.mvplugins.multiverse.core.command.MVCommandIssuer;
import org.mvplugins.multiverse.core.command.queue.CommandQueueManager;
import org.mvplugins.multiverse.core.command.queue.CommandQueuePayload;
import org.mvplugins.multiverse.core.locale.message.Message;
import org.mvplugins.multiverse.external.acf.commands.annotation.CommandPermission;
import org.mvplugins.multiverse.external.acf.commands.annotation.Subcommand;
import org.mvplugins.multiverse.external.jakarta.inject.Inject;
import org.mvplugins.multiverse.external.jetbrains.annotations.NotNull;
import org.mvplugins.multiverse.inventories.commands.InventoriesCommand;
import org.mvplugins.multiverse.inventories.config.InventoriesConfig;
import org.mvplugins.multiverse.inventories.profile.GlobalProfile;
import org.mvplugins.multiverse.inventories.profile.ProfileCacheManager;
import org.mvplugins.multiverse.inventories.profile.ProfileDataSource;
import org.mvplugins.multiverse.inventories.profile.key.GlobalProfileKey;
import org.mvplugins.multiverse.inventories.profile.key.ProfileKey;
import org.mvplugins.multiverse.inventories.profile.key.ProfileType;
import org.mvplugins.multiverse.inventories.profile.key.ProfileTypes;
import org.mvplugins.multiverse.inventories.profile.key.ContainerType;
import org.mvplugins.multiverse.inventories.profile.ProfileFilesLocator;
import org.mvplugins.multiverse.inventories.util.ItemStackConverter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
final class MigrateInventorySerializationCommand extends InventoriesCommand {

    private final CommandQueueManager commandQueueManager;
    private final ProfileDataSource profileDataSource;
    private final InventoriesConfig inventoriesConfig;
    private final ProfileCacheManager profileCacheManager;
    private final ProfileFilesLocator profileFilesLocator;

    @Inject
    MigrateInventorySerializationCommand(
            @NotNull CommandQueueManager commandQueueManager,
            @NotNull ProfileDataSource profileDataSource,
            @NotNull InventoriesConfig inventoriesConfig,
            @NotNull ProfileCacheManager profileCacheManager,
            @NotNull ProfileFilesLocator profileFilesLocator
    ) {
        this.commandQueueManager = commandQueueManager;
        this.profileDataSource = profileDataSource;
        this.inventoriesConfig = inventoriesConfig;
        this.profileCacheManager = profileCacheManager;
        this.profileFilesLocator = profileFilesLocator;
    }

    @Subcommand("bulkedit migrate inventory-serialization nbt")
    @CommandPermission("multiverse.inventories.bulkedit")
    void onNbtCommand(MVCommandIssuer issuer) {
        if (!ItemStackConverter.hasByteSerializeSupport()) {
            issuer.sendMessage("NBT serialization is only supported on PaperMC 1.20.2 or higher!");
            issuer.sendMessage("Conversion to NBT is not possible on your current server version.");
            return;
        }
        commandQueueManager.addToQueue(CommandQueuePayload.issuer(issuer)
                .prompt(Message.of("Are you sure you want to migrate all player data to NBT?"))
                .action(() -> doMigration(issuer, true)));
    }

    @Subcommand("bulkedit migrate inventory-serialization bukkit")
    @CommandPermission("multiverse.inventories.bulkedit")
    void onBukkitCommand(MVCommandIssuer issuer) {
        commandQueueManager.addToQueue(CommandQueuePayload.issuer(issuer)
                .prompt(Message.of("Are you sure you want to migrate all player data to old Bukkit serialization?"))
                .action(() -> doMigration(issuer, false)));
    }

    private void doMigration(MVCommandIssuer issuer, boolean useByteSerialization) {
        issuer.sendMessage("Updating config and clearing caches...");
        inventoriesConfig.setUseByteSerializationForInventoryData(useByteSerialization);
        inventoriesConfig.save();
        profileCacheManager.clearAllCache();

        long startTime = System.nanoTime();
        AtomicLong profileCounter = new AtomicLong(0);

        Map<ContainerType, List<String>> containerNames = new HashMap<>();
        for (ContainerType type : ContainerType.values()) {
            containerNames.put(type, profileDataSource.listContainerDataNames(type));
        }

        Set<String> playerIdentifiers = new HashSet<>();
        // Scan global files
        profileFilesLocator.listGlobalFiles().forEach(file ->
                playerIdentifiers.add(Files.getNameWithoutExtension(file.getName())));

        // Scan world and group files
        for (ContainerType type : ContainerType.values()) {
            for (File folder : profileFilesLocator.listProfileContainerFolders(type)) {
                profileFilesLocator.listPlayerProfileFiles(type, folder.getName()).forEach(file ->
                        playerIdentifiers.add(Files.getNameWithoutExtension(file.getName())));
            }
        }

        issuer.sendMessage("Found " + playerIdentifiers.size() + " unique player identifiers to migrate.");

        migrateNextPlayer(issuer, new ArrayList<>(playerIdentifiers), 0, containerNames, profileCounter)
                .thenRun(() -> {
                    long timeDuration = (System.nanoTime() - startTime) / 1000000;
                    issuer.sendMessage("Updated " + profileCounter.get() + " player profiles.");
                    issuer.sendMessage("Bulk edit completed in " + timeDuration + " ms.");
                });
    }

    private CompletableFuture<Void> migrateNextPlayer(
            MVCommandIssuer issuer,
            List<String> playerIdentifiers,
            int index,
            Map<ContainerType, List<String>> containerNames,
            AtomicLong profileCounter
    ) {
        if (index >= playerIdentifiers.size()) {
            return CompletableFuture.completedFuture(null);
        }

        String playerIdentifier = playerIdentifiers.get(index);
        UUID playerUUID;
        try {
            playerUUID = UUID.fromString(playerIdentifier);
        } catch (IllegalArgumentException e) {
            playerUUID = UUID.nameUUIDFromBytes(("OfflinePlayer:" + playerIdentifier).getBytes(StandardCharsets.UTF_8));
        }

        if (index % 50 == 0 && index > 0) {
            issuer.sendMessage("Processed " + index + " players...");
        }

        return profileDataSource.getGlobalProfile(GlobalProfileKey.of(playerUUID, playerIdentifier))
                .thenCompose(profile -> run(issuer, profile, containerNames, profileCounter))
                .exceptionally(throwable -> {
                    issuer.sendMessage("Error updating player " + playerIdentifier + ": " + throwable.getMessage());
                    return null;
                })
                .thenCompose(v -> migrateNextPlayer(issuer, playerIdentifiers, index + 1, containerNames, profileCounter));
    }

    private CompletableFuture<Void> run(
            MVCommandIssuer issuer,
            GlobalProfile profile,
            Map<ContainerType, List<String>> containerNames,
            AtomicLong profileCounter
    ) {
        String playerName = profile.getLastKnownName();
        if (playerName == null || playerName.isEmpty()) {
            playerName = profile.getPlayerUUID().toString();
        }

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (ContainerType containerType : ContainerType.values()) {
            List<String> dataNames = containerNames.get(containerType);
            for (String dataName : dataNames) {
                for (ProfileType profileType : ProfileTypes.getTypes()) {
                    ProfileKey profileKey = ProfileKey.of(
                            containerType,
                            dataName,
                            profileType,
                            profile.getPlayerUUID(),
                            playerName
                    );
                    future = future.thenCompose(v -> profileDataSource.getPlayerProfile(profileKey)
                            .thenCompose(playerProfile -> {
                                if (playerProfile.getData().isEmpty()) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                profileCounter.incrementAndGet();
                                return profileDataSource.updatePlayerProfile(playerProfile);
                            })
                            .exceptionally(throwable -> {
                                issuer.sendMessage(String.format("Error migrating profile %s %s/%s for player %s: %s",
                                        containerType, dataName, profileType, profile.getPlayerUUID(), throwable.getMessage()));
                                return null;
                            })
                    );
                }
            }
        }
        return future;
    }
}
