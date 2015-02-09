/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.cli.commandset;

import co.cask.cdap.authorization.ACLManagerClient;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.TypedId;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.io.PrintStream;
import java.net.URI;
import java.util.Set;

/**
 * Management commands.
 */
public class ManagementCommands extends CommandSet<Command> {

  @Inject
  public ManagementCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(CreateACLEntryCommand.class))
        .add(injector.getInstance(DeleteACLEntryCommand.class))
        .add(injector.getInstance(ListACLEntriesCommand.class))
        .add(injector.getInstance(SearchACLEntriesCommand.class))
        .add(injector.getInstance(CreateACLEntryCommand.class))
        .add(injector.getInstance(DeleteACLEntryCommand.class))
        .build());
  }

  private static TypedId fromString(String typedId) {
    if (typedId == null) {
      return null;
    }

    if (ObjectId.GLOBAL.getRep().equals(typedId)) {
      return ObjectId.GLOBAL;
    }

    String[] parts = typedId.split("\\.");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid object/subject ID format");
    }

    return new TypedId(parts[0], parts[1]);
  }

  private static void printACLs(Iterable<ACLEntry> entries, PrintStream printStream) {
    new AsciiTable<ACLEntry>(new String[] { "object", "subject", "permission" }, entries, new RowMaker<ACLEntry>() {
      @Override
      public Object[] makeRow(ACLEntry object) {
        return new Object[] {
          customToString(object.getObject()),
          customToString(object.getSubject()),
          object.getPermission().getName()
        };
      }
    }).print(printStream);
  }

  private static Object customToString(SubjectId subject) {
    return subject.getRep();
  }

  private static String customToString(ObjectId object) {
    return object.getRep();
  }

  /**
   *
   */
  private static final class ListACLEntriesCommand extends AbstractAuthCommand {

    private final ACLManagerClient client;

    @Inject
    public ListACLEntriesCommand(final CLIConfig cliConfig) {
      super(cliConfig);
      this.client = new ACLManagerClient(new Supplier<URI>() {
        @Override
        public URI get() {
          return cliConfig.getURI();
        }
      });
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      ACLStore.Query query = new ACLStore.Query(null, null, null);
      Set<ACLEntry> entries = client.getACLs(cliConfig.getCurrentNamespace(), query);
      printACLs(entries, printStream);
    }

    @Override
    public String getPattern() {
      return "list acls";
    }

    @Override
    public String getDescription() {
      return "Lists all ACL entries";
    }
  }

  /**
   *
   */
  private static final class SearchACLEntriesCommand extends AbstractAuthCommand {

    private final ACLManagerClient client;

    @Inject
    public SearchACLEntriesCommand(final CLIConfig cliConfig) {
      super(cliConfig);
      this.client = new ACLManagerClient(new Supplier<URI>() {
        @Override
        public URI get() {
          return cliConfig.getURI();
        }
      });
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String objectTypeAndId = arguments.get("object-type.object-id", null);
      ObjectId objectId = new ObjectId(fromString(objectTypeAndId));

      String subjectTypeAndId = arguments.get("subject-type.subject-id", null);
      SubjectId subjectId = new SubjectId(fromString(subjectTypeAndId));

      ACLStore.Query query = new ACLStore.Query(objectId, subjectId, null);
      Set<ACLEntry> entries = client.getACLs(cliConfig.getCurrentNamespace(), query);
      printACLs(entries, printStream);
    }

    @Override
    public String getPattern() {
      return "search acls [object <object-type.object-id>] [subject <subject-type.subject-id>]";
    }

    @Override
    public String getDescription() {
      return "Searches for ACL entries matching the specified parameters";
    }
  }

  /**
   *
   */
  private static final class CreateACLEntryCommand extends AbstractAuthCommand {

    private final ACLManagerClient client;

    @Inject
    public CreateACLEntryCommand(final CLIConfig cliConfig) {
      super(cliConfig);
      this.client = new ACLManagerClient(new Supplier<URI>() {
        @Override
        public URI get() {
          return cliConfig.getURI();
        }
      });
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String objectTypeAndId = arguments.get("object-type.object-id");
      ObjectId objectId = new ObjectId(fromString(objectTypeAndId));

      String subjectTypeAndId = arguments.get("subject-type.subject-id");
      SubjectId subjectId = new SubjectId(fromString(subjectTypeAndId));

      Permission permission = Permission.fromName(arguments.get("permission"));

      client.createACL(cliConfig.getCurrentNamespace(), new ACLEntry(objectId, subjectId, permission));
    }

    @Override
    public String getPattern() {
      return "create acl object <object-type.object-id> subject <subject-type.subject-id> permission <permission>";
    }

    @Override
    public String getDescription() {
      return "Creates an ACL entry";
    }
  }

  /**
   *
   */
  private static final class DeleteACLEntryCommand extends AbstractAuthCommand {

    private final ACLManagerClient client;

    @Inject
    public DeleteACLEntryCommand(final CLIConfig cliConfig) {
      super(cliConfig);
      this.client = new ACLManagerClient(new Supplier<URI>() {
        @Override
        public URI get() {
          return cliConfig.getURI();
        }
      });
    }

    @Override
    public void perform(Arguments arguments, PrintStream printStream) throws Exception {
      String objectTypeAndId = arguments.get("object-type.object-id");
      ObjectId objectId = new ObjectId(fromString(objectTypeAndId));

      String subjectTypeAndId = arguments.get("subject-type.subject-id");
      SubjectId subjectId = new SubjectId(fromString(subjectTypeAndId));

      Permission permission = Permission.fromName(arguments.get("permission"));

      client.deleteACLs(cliConfig.getCurrentNamespace(), new ACLStore.Query(objectId, subjectId, permission));
    }

    @Override
    public String getPattern() {
      return "delete acl object <object-type.object-id> subject <subject-type.subject-id> permission <permission>";
    }

    @Override
    public String getDescription() {
      return "Deletes an ACL entry";
    }
  }
}
